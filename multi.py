# /// script
# dependencies = [
#   "azure-messaging-webpubsubclient==1.1.0",
#   "protobuf==6.33.1",
#   "requests==2.32.5",
#   "zstandard==0.25.0",
# ]
# ///

"""
Quant websocket discovery example.

This follows a typical Quant user flow:

1. Query the standard ticker list and choose SPX/NDX.
2. Confirm the Classic gex_full and State gex_full/gamma_zero categories.
3. Query the Quant ticker list and choose the first five supplemental tickers.
4. Query each selected Quant ticker's valid expirations within the published
   90-day horizon.
5. Subscribe to:
   - Classic gex_full for every selected ticker, which supplies zero_gamma
   - State gex_full for every selected ticker, which supplies major positive/negative
   - State gamma_zero for SPX/NDX
   - State gamma profiles for the next three valid expirations of each Quant ticker,
     which supply major_long_gamma and major_short_gamma
   - One matching {ticker}_spot group on each V2 hub used by that ticker

Analytics and spot use separate groups on the same V2 connection.
Groups are joined server-side by POST /v2/negotiate. The client should not send
Azure joinGroup messages when using this flow.
"""

import json
import os
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from azure.messaging.webpubsubclient import WebPubSubClient
from azure.messaging.webpubsubclient.models import (
    CallbackType,
    OnConnectedArgs,
    OnDisconnectedArgs,
    OnGroupDataMessageArgs,
)
from google.protobuf import any_pb2

from decompression_utils import (
    decompress_gex_message,
    decompress_greek_message,
    decompress_spot_message,
)
from group_config import (
    add_colocated_spot_groups,
    build_post_groups,
    count_group_memberships,
)

SCRIPT_DIR = Path(__file__).resolve().parent

STANDARD_PROFILE_TICKERS = ["SPX", "NDX"]
CLASSIC_GEX_CATEGORY = "gex_full"
STATE_GEX_CATEGORY = "gex_full"
STANDARD_STATE_GAMMA_CATEGORY = "gamma_zero"
CLASSIC_HUB = "v2_classic"
STATE_GEX_HUB = "v2_state_gex"
STANDARD_STATE_GAMMA_HUB = "v2_state_greeks_zero"
EXPIRY_STATE_GAMMA_HUB = "v2_state_greeks"
QUANT_TICKER_COUNT = 5
EXPIRATIONS_PER_QUANT_TICKER = 3
QUANT_GROUP_LIMIT = 150


def load_dotenv_file(path: Path) -> None:
    """Tiny .env loader so this can use quant-sockets/.env without extra deps."""
    if not path.exists():
        return

    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


load_dotenv_file(SCRIPT_DIR / ".env")


def required_environment_variable(name: str) -> str:
    """Return one required environment variable or raise a clear error."""
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise RuntimeError(f"{name} is required and must not be empty.")
    return value.strip()


RAW_BASE_URL = required_environment_variable("BASE_URL")
API_KEY = required_environment_variable("GEXBOT_API_KEY")
USER_AGENT = required_environment_variable("GEXBOT_USER_AGENT")


def api_roots(raw_base_url: str) -> Tuple[str, str]:
    """Return (root_base, v2_base) from either a root or /v2 base URL."""
    base = raw_base_url.rstrip("/")
    if base.endswith("/v2"):
        return base[:-3], base
    return base, f"{base}/v2"


ROOT_BASE_URL, V2_BASE_URL = api_roots(RAW_BASE_URL)
STANDARD_TICKERS_URL = f"{V2_BASE_URL}/tickers"
CLASSIC_CATEGORIES_URL = f"{ROOT_BASE_URL}/classic/categories"
STATE_CATEGORIES_URL = f"{ROOT_BASE_URL}/state/categories"
QUANT_TICKERS_URL = f"{ROOT_BASE_URL}/tickers/quant"
NEGOTIATE_URL = f"{V2_BASE_URL}/negotiate"


def expirations_url(ticker: str) -> str:
    """Build the V2 expiration-discovery URL for one ticker."""
    return f"{V2_BASE_URL}/options/{ticker}/expiries"


def build_auth_headers(content_type_json: bool = False) -> Dict[str, str]:
    """Build the required API headers for a JSON or read request."""
    if not API_KEY or API_KEY == "your_api_key_here":
        raise ValueError("GEXBOT_API_KEY is not set.")

    if not USER_AGENT.strip():
        raise ValueError("GEXBOT_USER_AGENT is not set.")

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "User-Agent": USER_AGENT.strip(),
        "Accept": "application/json",
    }
    if content_type_json:
        headers["Content-Type"] = "application/json"
    return headers


def get_json(url: str) -> Dict:
    """Get and decode one authenticated JSON response."""
    print(f"GET {url}")
    response = requests.get(url, headers=build_auth_headers(), timeout=30)
    if not response.ok:
        print(response.text)
    response.raise_for_status()
    return response.json()


def post_negotiate(group_config: Dict[str, List[str]]) -> Dict:
    """Negotiate V2 connections with analytics and one spot request per ticker."""
    groups = build_post_groups(group_config)
    payload = {"groups": groups}
    print(f"\nPOST {NEGOTIATE_URL} with {len(groups)} group(s)")
    response = requests.post(
        NEGOTIATE_URL,
        headers=build_auth_headers(content_type_json=True),
        json=payload,
        timeout=30,
    )
    if not response.ok:
        print(response.text)
    response.raise_for_status()
    return response.json()


def normalize_tickers(payload: Dict) -> List[str]:
    """Return a stable and deduplicated ticker list from a discovery response."""
    tickers: List[str] = []
    for key in ("stocks", "indexes", "futures", "pubsub_only"):
        values = payload.get(key) or []
        if isinstance(values, list):
            tickers.extend(
                str(value).strip().upper() for value in values if str(value).strip()
            )

    seen = set()
    normalized: List[str] = []
    for ticker in tickers:
        if ticker not in seen:
            normalized.append(ticker)
            seen.add(ticker)
    return normalized


def to_yyyymmdd(expiration: str) -> str:
    """Convert one ISO expiration date to a WebSocket group suffix."""
    return expiration.replace("-", "")


def select_standard_profile_tickers() -> List[str]:
    """Select the configured standard tickers from the public ticker response."""
    payload = get_json(STANDARD_TICKERS_URL)
    available = set(normalize_tickers(payload))
    selected = [ticker for ticker in STANDARD_PROFILE_TICKERS if ticker in available]
    missing = [ticker for ticker in STANDARD_PROFILE_TICKERS if ticker not in available]

    print("\nStandard ticker list response counts:")
    for key in ("stocks", "indexes", "futures"):
        values = payload.get(key) or []
        print(f"  {key}: {len(values)}")

    if missing:
        raise RuntimeError(
            f"Standard ticker list did not include: {', '.join(missing)}"
        )

    print(f"Selected standard profile tickers: {', '.join(selected)}")
    return selected


def confirm_categories(
    package_name: str, url: str, expected_categories: List[str]
) -> None:
    """Confirm that one package advertises all categories used by the example."""
    payload = get_json(url)
    if not isinstance(payload, list):
        raise RuntimeError(f"{package_name} categories response was not a list.")

    categories = [str(category) for category in payload]
    print(f"\n{package_name} categories: {', '.join(categories)}")
    missing = [
        category for category in expected_categories if category not in categories
    ]
    if missing:
        raise RuntimeError(
            f"{package_name} categories did not include: {', '.join(missing)}."
        )


def select_quant_tickers() -> List[str]:
    """Select a bounded set of supplemental Quant tickers."""
    payload = get_json(QUANT_TICKERS_URL)
    print("\nQuant ticker response:")
    print(json.dumps(payload, indent=2))

    tickers = normalize_tickers(payload)
    selected = tickers[:QUANT_TICKER_COUNT]
    if not selected:
        raise RuntimeError("No Quant tickers returned.")

    print(f"Selected first {len(selected)} Quant tickers: {', '.join(selected)}")
    return selected


def fetch_next_expirations(tickers: List[str]) -> Dict[str, List[str]]:
    """Get the next configured number of valid expirations for each ticker."""
    expirations_by_ticker: Dict[str, List[str]] = {}

    print("\nNext valid expirations for selected Quant tickers:")
    for ticker in tickers:
        payload = get_json(expirations_url(ticker))
        expirations = sorted(
            {str(expiration) for expiration in payload.get("expiries", [])}
        )
        selected = expirations[:EXPIRATIONS_PER_QUANT_TICKER]
        expirations_by_ticker[ticker] = selected

        print(f"  {ticker}: {', '.join(selected) or 'none'}")

    return expirations_by_ticker


def build_group_config(
    standard_tickers: List[str],
    quant_expirations: Dict[str, List[str]],
) -> Dict[str, List[str]]:
    """Build V2 analytics and colocated spot memberships for the example."""
    all_tickers = list(dict.fromkeys([*standard_tickers, *quant_expirations.keys()]))
    classic_groups = [
        f"{ticker}_classic_{CLASSIC_GEX_CATEGORY}" for ticker in all_tickers
    ]
    state_gex_groups = [
        f"{ticker}_state_{STATE_GEX_CATEGORY}" for ticker in all_tickers
    ]
    standard_state_gamma_groups = [
        f"{ticker}_state_{STANDARD_STATE_GAMMA_CATEGORY}" for ticker in standard_tickers
    ]
    expiry_state_gamma_groups: List[str] = []

    for ticker, expirations in quant_expirations.items():
        for expiration in expirations:
            expiry_state_gamma_groups.append(
                f"{ticker}_state_gamma_{to_yyyymmdd(expiration)}"
            )

    analytics_group_config = {
        CLASSIC_HUB: classic_groups,
        STATE_GEX_HUB: state_gex_groups,
        STANDARD_STATE_GAMMA_HUB: standard_state_gamma_groups,
        EXPIRY_STATE_GAMMA_HUB: expiry_state_gamma_groups,
    }
    return add_colocated_spot_groups(analytics_group_config)


def print_group_config(group_config: Dict[str, List[str]]) -> None:
    """Print analytics and spot memberships for each selected V2 hub."""
    print("\nV2 memberships selected for POST /v2/negotiate:")
    for hub, groups in group_config.items():
        print(f"  [{hub}]")
        for group in groups:
            print(f"    {group}")
    print(
        f"Total hub memberships: "
        f"{count_group_memberships(group_config)}/{QUANT_GROUP_LIMIT}"
    )


class WebPubSubClientManager:
    def __init__(self, hub_key: str, connection_url: str, subscribed_groups: List[str]):
        """Create one manager for a signed V2 hub connection URL."""
        self.hub_key = hub_key
        self.subscribed_groups = subscribed_groups
        self.client = WebPubSubClient(connection_url)
        self.thread: Optional[threading.Thread] = None

        self.client.subscribe(CallbackType.CONNECTED, self.on_connected)
        self.client.subscribe(CallbackType.DISCONNECTED, self.on_disconnected)
        self.client.subscribe(CallbackType.GROUP_MESSAGE, self.on_group_message)

    def start(self) -> None:
        """Open the managed connection on a background thread."""
        print(f"[{self.hub_key}] starting client")
        self.thread = threading.Thread(target=self.client.open, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        """Close the managed connection."""
        print(f"[{self.hub_key}] stopping client")
        self.client.close()

    def on_connected(self, event: OnConnectedArgs) -> None:
        """Report a successful Web PubSub connection."""
        print(f"[{self.hub_key}] ✅ connected (ID: {event.connection_id})")
        print(f"[{self.hub_key}] subscribed: {', '.join(self.subscribed_groups)}")

    def on_disconnected(self, event: OnDisconnectedArgs) -> None:
        """Report a closed Web PubSub connection."""
        print(f"[{self.hub_key}] ❌ disconnected: {event.message}")

    def on_group_message(self, event: OnGroupDataMessageArgs) -> None:
        """Decode and print one analytics or spot group message."""
        try:
            any_message = any_pb2.Any()
            any_message.ParseFromString(event.data)
            message_type_url = any_message.type_url

            current_category = ""
            for package_name in ("classic", "state"):
                separator = f"_{package_name}_"
                if separator in event.group:
                    current_category = event.group.split(separator, 1)[-1]
                    break

            if "proto.spot" in message_type_url:
                spot_data = decompress_spot_message(any_message)
                print(
                    f"[{self.hub_key}] {event.group}: "
                    f"{spot_data.get('ticker')} spot={spot_data.get('spot')} "
                    f"timestamp={spot_data.get('timestamp')}"
                )
                return

            if "proto.gex" in message_type_url:
                gex_data = decompress_gex_message(any_message)
                if "_classic_" in event.group:
                    print(
                        f"[{self.hub_key}] {event.group}: "
                        f"{gex_data.get('ticker')} "
                        f"zero_gamma={gex_data.get('zero_gamma')}"
                    )
                    return

                if "_state_gex_" in event.group:
                    print(
                        f"[{self.hub_key}] {event.group}: "
                        f"{gex_data.get('ticker')} "
                        f"major_positive={gex_data.get('major_pos_vol')} "
                        f"major_negative={gex_data.get('major_neg_vol')}"
                    )
                    return

            if "proto.greek" in message_type_url:
                greek_data = decompress_greek_message(any_message, current_category)
                print(
                    f"[{self.hub_key}] {event.group}: "
                    f"{greek_data.get('ticker')} "
                    f"major_long_gamma={greek_data.get('major_long_gamma')} "
                    f"major_short_gamma={greek_data.get('major_short_gamma')}"
                )
                return

            print(f"[{self.hub_key}] unknown message type_url: {message_type_url}")
        except Exception as exc:
            group = getattr(event, "group", "?")
            print(f"[{self.hub_key}] failed to parse message from {group}: {exc}")


def start_clients(
    websocket_urls: Dict[str, str],
    group_config: Dict[str, List[str]],
) -> Dict[str, WebPubSubClientManager]:
    """Open one managed client for each V2 hub used by the group configuration."""
    managers: Dict[str, WebPubSubClientManager] = {}

    for hub_key, groups in group_config.items():
        url = websocket_urls.get(hub_key)
        if not url:
            raise RuntimeError(f"No websocket URL returned for hub {hub_key}.")

        manager = WebPubSubClientManager(hub_key, url, groups)
        manager.start()
        managers[hub_key] = manager

    return managers


def main() -> None:
    """Discover Quant data, negotiate V2 memberships, and print live messages."""
    managers: Dict[str, WebPubSubClientManager] = {}

    try:
        print(f"Using API root: {ROOT_BASE_URL}")
        print(f"Using v2 API:   {V2_BASE_URL}")

        standard_tickers = select_standard_profile_tickers()
        confirm_categories("Classic", CLASSIC_CATEGORIES_URL, [CLASSIC_GEX_CATEGORY])
        confirm_categories(
            "State",
            STATE_CATEGORIES_URL,
            [STATE_GEX_CATEGORY, STANDARD_STATE_GAMMA_CATEGORY],
        )
        quant_tickers = select_quant_tickers()
        quant_expirations = fetch_next_expirations(quant_tickers)

        group_config = build_group_config(standard_tickers, quant_expirations)
        membership_count = count_group_memberships(group_config)
        if membership_count == 0:
            raise RuntimeError("No websocket groups could be built.")
        if membership_count > QUANT_GROUP_LIMIT:
            raise RuntimeError(
                f"Selected {membership_count} memberships, exceeding the "
                f"{QUANT_GROUP_LIMIT}-group Quant limit."
            )

        print_group_config(group_config)
        negotiate_data = post_negotiate(group_config)
        websocket_urls = negotiate_data.get("websocket_urls")
        if not websocket_urls:
            raise RuntimeError("No websocket_urls returned from negotiate.")

        print("\nStarting WebSocket clients. Messages publish during market hours.")
        managers = start_clients(websocket_urls, group_config)

        print("Running until Ctrl+C...")
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        pass
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error: {http_err}")
        if http_err.response is not None:
            print(http_err.response.text)
    except Exception as exc:
        print(f"Error: {exc}")
    finally:
        if managers:
            print("\nStopping clients...")
            for manager in managers.values():
                manager.stop()


if __name__ == "__main__":
    main()
