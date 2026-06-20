import requests
import os
import json
import threading
import time
from typing import Dict, List, Optional

# --- Azure Web PubSub Client ---
from azure.messaging.webpubsubclient import WebPubSubClient
from azure.messaging.webpubsubclient.models import (
    OnConnectedArgs,
    OnDisconnectedArgs,
    OnGroupDataMessageArgs,
    CallbackType,
)

# --- Protobuf Imports ---
from google.protobuf import any_pb2

# --- Local Decompression Imports ---
# Import the functions from your new file
from decompression_utils import (
    decompress_gex_message,
    decompress_greek_message,
    decompress_orderflow_message
)
# -----------------------------------

# --- Configuration ---

# Get API key from an environment variable for security.
API_KEY = os.environ.get("GEXBOT_API_DEV_KEY", "")

# Required by the Gexbot API. Set this to a value that identifies your
# client application before running the script.
USER_AGENT = os.environ.get("GEXBOT_USER_AGENT", "")

BASE_URL = "https://dev-api.gex.bot/v2"
NEGOTIATE_URL = f"{BASE_URL}/negotiate"

# --- USER SELECTION: Uncomment the feeds you want to subscribe to ---

# Select Tickers (used for all hubs)
ACTIVE_TICKERS = [
    "SPX",
    # "ES_SPX",
    "NDX",
    # "NQ_NDX",
    # "RUT",
    # "SPY",
    # "QQQ",
]

# Select categories for the 'classic' hub
ACTIVE_CLASSIC_CATEGORIES = [
    # "gex_full",
    # "gex_zero",
    # "gex_one",
]

# Select categories for the 'state_gex' hub
ACTIVE_STATE_GEX_CATEGORIES = [
    "gex_full",
    "gex_zero",
    "gex_one",
]

# Select categories for the 'state_greeks_zero' hub
ACTIVE_STATE_GREEKS_ZERO_CATEGORIES = [
    "delta_zero",
    "gamma_zero",
    "vanna_zero",
    # "charm_zero",
]

# Select categories for the 'state_greeks_one' hub
ACTIVE_STATE_GREEKS_ONE_CATEGORIES = [
    # "delta_one",
    # "gamma_one",
    # "vanna_one",
    # "charm_one",
]

# Select categories for the 'orderflow' hub
ACTIVE_ORDERFLOW_CATEGORIES = [
    # "orderflow",
]

# --- End of USER SELECTION ---


# --- Group Name Generation ---

def _generate_group_names(tickers: List[str], package: str, categories: List[str]) -> List[str]:
    """Create unprefixed websocket group names for POST /negotiate."""
    groups = []
    for ticker in tickers:
        for category in categories:
            groups.append(f"{ticker}_{package}_{category}")
    return groups


def build_group_config() -> Dict[str, List[str]]:
    """Build groups by hub from the USER SELECTION section above."""
    return {
        "classic": _generate_group_names(
            ACTIVE_TICKERS, "classic", ACTIVE_CLASSIC_CATEGORIES
        ),
        "state_gex": _generate_group_names(
            ACTIVE_TICKERS, "state", ACTIVE_STATE_GEX_CATEGORIES
        ),
        "state_greeks_zero": _generate_group_names(
            ACTIVE_TICKERS, "state", ACTIVE_STATE_GREEKS_ZERO_CATEGORIES
        ),
        "state_greeks_one": _generate_group_names(
            ACTIVE_TICKERS, "state", ACTIVE_STATE_GREEKS_ONE_CATEGORIES
        ),
        "orderflow": _generate_group_names(
            ACTIVE_TICKERS, "orderflow", ACTIVE_ORDERFLOW_CATEGORIES
        ),
    }


def flatten_group_config(group_config: Dict[str, List[str]]) -> List[str]:
    """Flatten hub -> groups while preserving the configured order."""
    return [group for groups in group_config.values() for group in groups]


# --- Web PubSub Client Manager ---

class WebPubSubClientManager:
    """
    Manages a single Web PubSub client connection and event handling.

    Groups are requested during POST /negotiate and embedded in the returned
    access token, so this client does not call join_group after connecting.
    """

    def __init__(self, hub_key: str, connection_url: str, subscribed_groups: List[str]):
        self.hub_key = hub_key
        self.subscribed_groups = subscribed_groups
        self.client = WebPubSubClient(connection_url)
        self.thread: Optional[threading.Thread] = None

        self.client.subscribe(CallbackType.CONNECTED, self.on_connected)
        self.client.subscribe(CallbackType.DISCONNECTED, self.on_disconnected)
        self.client.subscribe(CallbackType.GROUP_MESSAGE,
                              self.on_group_message)

    def start(self):
        print(f"[{self.hub_key}] Starting client thread...")
        self.thread = threading.Thread(target=self.client.open, daemon=True)
        self.thread.start()

    def stop(self):
        print(f"[{self.hub_key}] Stopping client...")
        self.client.close()

    def on_connected(self, event: OnConnectedArgs):
        print(f"[{self.hub_key}] ✅ Web PubSub connected (ID: {event.connection_id})")
        if self.subscribed_groups:
            print(
                f"[{self.hub_key}] 📢 Subscribed via negotiate: "
                f"{', '.join(self.subscribed_groups)}"
            )

    def on_disconnected(self, event: OnDisconnectedArgs):
        print(f"[{self.hub_key}] ❌ Web PubSub disconnected: {event.message}")

    def on_group_message(self, event: OnGroupDataMessageArgs):
        try:
            any_message = any_pb2.Any()
            any_message.ParseFromString(event.data)
            message_type_url = any_message.type_url

            # Format: {server_prefix}_{ticker}_{package}_{category}
            current_category = ""
            known_packages = ["classic", "state", "orderflow"]

            for pkg in known_packages:
                separator = f"_{pkg}_"
                if separator in event.group:
                    current_category = event.group.split(separator)[-1]
                    break

            if not current_category:
                print(
                    f"  ⚠️ Could not extract category from group: {event.group}")
                return

            if "proto.gex" in message_type_url:
                gex_data = decompress_gex_message(any_message)
                if gex_data:
                    print(
                        f"[{self.hub_key}] GEX: {gex_data.get('ticker')} @ {gex_data.get('spot')}")

            elif "proto.greek" in message_type_url:
                greek_data = decompress_greek_message(
                    any_message, current_category)
                if greek_data:
                    if "mini_contracts" in greek_data:
                        print(
                            f"[{self.hub_key}] {current_category}: {greek_data.get('ticker')} (JSON path)")
                    else:
                        print(
                            f"[{self.hub_key}] {current_category}: {greek_data.get('ticker')} (Proto path)")

            elif "proto.orderflow" in message_type_url:
                orderflow_data = decompress_orderflow_message(any_message)
                if orderflow_data:
                    print(
                        f"[{self.hub_key}] Orderflow: {orderflow_data.get('spot')}")

            else:
                print(f"  Unknown message type_url: {message_type_url}")

        except Exception as e:
            print(f"  Failed to parse protobuf message: {e}")


# --- Negotiation Function ---

def get_negotiate_response(api_key: str, user_agent: str, groups: List[str]) -> Optional[Dict]:
    """
    POSTs requested websocket groups to /negotiate using an API key for authentication.
    """
    if not api_key or api_key == "your_api_key_here":
        print("Error: GEXBOT_API_KEY is not set.")
        return None

    user_agent = user_agent.strip()
    if not user_agent:
        print("Error: GEXBOT_USER_AGENT must be set before running this script.")
        print('Example: export GEXBOT_USER_AGENT="AcmeQuantClient/1.0"')
        return None

    if not groups:
        print("Error: No websocket groups selected. Update the USER SELECTION section in main.py.")
        return None

    headers = {
        "Authorization": f"Bearer {api_key}",
        "User-Agent": user_agent,
    }
    payload = {"groups": groups}
    print(f"POST {NEGOTIATE_URL} with {len(groups)} configured group(s)...")

    try:
        response = requests.post(
            NEGOTIATE_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.HTTPError as http_err:
        print(f"Negotiation failed: {http_err}")
        if http_err.response is not None:
            print(http_err.response.text)
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred: {req_err}")
    return None


# --- Main Script ---
if __name__ == "__main__":
    GROUP_CONFIG = build_group_config()
    requested_groups = flatten_group_config(GROUP_CONFIG)

    print("\n--- Requested WebSocket Groups ---")
    for hub_key, groups in GROUP_CONFIG.items():
        if groups:
            print(f"[{hub_key}] {', '.join(groups)}")

    negotiate_data = get_negotiate_response(
        API_KEY, USER_AGENT, requested_groups)

    if not (negotiate_data and 'websocket_urls' in negotiate_data):
        print("\n--- Negotiation Failed or 'websocket_urls' key missing ---")
        if negotiate_data:
            print(json.dumps(negotiate_data, indent=2))
        exit()

    print("\n--- Successfully Negotiated ---")

    websocket_urls_dict = negotiate_data['websocket_urls']

    # --- Start Clients ---
    print("\n--- Initializing WebSocket Clients based on negotiated hubs ---")
    client_managers: List[WebPubSubClientManager] = []

    for hub_key, url in websocket_urls_dict.items():
        subscribed_groups = GROUP_CONFIG.get(hub_key, [])

        if subscribed_groups:
            print(
                f"[Main] Found {len(subscribed_groups)} negotiated group(s) for hub: {hub_key}")
            manager = WebPubSubClientManager(hub_key, url, subscribed_groups)
            manager.start()
            client_managers.append(manager)
        else:
            print(
                f"[Main] Skipping {hub_key}: No groups defined in GROUP_CONFIG")

    if not client_managers:
        print("\nNo clients were started. Check your GROUP_CONFIG and negotiate response.")
        exit()

    print("\nClients are running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n--- Stopping Clients ---")
        for manager in client_managers:
            manager.stop()
        print("All clients stopped. Exiting.")
