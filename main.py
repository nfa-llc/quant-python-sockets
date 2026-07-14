# /// script
# dependencies = [
#   "azure-messaging-webpubsubclient==1.1.0",
#   "protobuf==6.33.1",
#   "requests==2.32.5",
#   "zstandard==0.25.0",
# ]
# ///

import json
import os
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

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
    decompress_orderflow_message,
)

# --- Configuration ---

SCRIPT_DIR = Path(__file__).resolve().parent


def load_dotenv_file(path: Path) -> None:
    """Load local environment variables without requiring an extra dependency."""
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

API_KEY = os.environ.get("GEXBOT_API_KEY", "")
USER_AGENT = os.environ.get("GEXBOT_USER_AGENT", "")
BASE_URL = os.environ.get("BASE_URL", "https://api.gex.bot/v2")
NEGOTIATE_URL = f"{BASE_URL}/negotiate"

# POST /negotiate initial groups. Group names are unprefixed:
# {ticker}_{package}_{category}
INITIAL_GROUP_CONFIG: Dict[str, List[str]] = {
    "classic": [],
    "state_gex": [
        "SPX_state_gex_full",
        "SPX_state_gex_zero",
    ],
    "state_greeks_zero": [
        "SPX_state_gamma_zero",
    ],
    "state_greeks_one": [],
    "orderflow": [],
}

# PATCH /negotiate replacement groups. This is a full replacement: any group not
# included here is removed from the active subscription set.
UPDATED_GROUP_CONFIG: Dict[str, List[str]] = {
    "classic": [
        "SPX_classic_gex_full",
    ],
    "state_gex": [
        "NDX_state_gex_full",
    ],
    "state_greeks_zero": [
        "SPX_state_delta_zero",
        "SPX_state_gamma_zero",
    ],
    "state_greeks_one": [],
    "orderflow": [],
}

# Set to 0 to skip the PATCH replacement example.
PATCH_UPDATE_AFTER_SECONDS = 20


def flatten_group_config(group_config: Dict[str, List[str]]) -> List[str]:
    """Flatten hub -> groups for POST /negotiate."""
    return [group for groups in group_config.values() for group in groups]


def build_group_update_payload(group_config: Dict[str, List[str]]) -> List[Dict[str, str]]:
    """Build hub-qualified groups for PATCH /negotiate."""
    return [
        {"hub": hub_key, "group": group}
        for hub_key, groups in group_config.items()
        for group in groups
    ]


def print_group_config(title: str, group_config: Dict[str, List[str]]):
    print(f"\n--- {title} ---")
    total = 0
    for hub_key, groups in group_config.items():
        total += len(groups)
        if groups:
            print(f"[{hub_key}] {', '.join(groups)}")
    print(f"Total groups: {total}")


def build_auth_headers(api_key: str, user_agent: str) -> Dict[str, str]:
    if not api_key or api_key == "your_api_key_here":
        raise ValueError("GEXBOT_API_KEY is not set.")

    if not user_agent.strip():
        raise ValueError('GEXBOT_USER_AGENT is not set. Example: "AcmeQuantClient/1.0"')

    return {
        "Authorization": f"Bearer {api_key}",
        "User-Agent": user_agent.strip(),
        "Accept": "application/json",
    }


def post_negotiate(api_key: str, user_agent: str, group_config: Dict[str, List[str]]) -> Dict:
    groups = flatten_group_config(group_config)
    if not groups:
        raise ValueError("No initial websocket groups configured.")

    headers = build_auth_headers(api_key, user_agent)
    payload = {"groups": groups}

    print(f"POST {NEGOTIATE_URL} with {len(groups)} group(s)...")
    response = requests.post(NEGOTIATE_URL, headers=headers, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def patch_replace_groups(api_key: str, user_agent: str, group_config: Dict[str, List[str]]) -> Dict:
    groups = build_group_update_payload(group_config)
    headers = build_auth_headers(api_key, user_agent)
    payload = {"groups": groups}

    print(f"PATCH {NEGOTIATE_URL} with {len(groups)} group(s)...")
    response = requests.patch(NEGOTIATE_URL, headers=headers, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


class WebPubSubClientManager:
    """Manages one Azure Web PubSub hub connection."""

    def __init__(self, hub_key: str, connection_url: str, subscribed_groups: List[str]):
        self.hub_key = hub_key
        self.subscribed_groups = subscribed_groups
        self.client = WebPubSubClient(connection_url)
        self.thread: Optional[threading.Thread] = None

        self.client.subscribe(CallbackType.CONNECTED, self.on_connected)
        self.client.subscribe(CallbackType.DISCONNECTED, self.on_disconnected)
        self.client.subscribe(CallbackType.GROUP_MESSAGE, self.on_group_message)

    def start(self):
        print(f"[{self.hub_key}] Starting client...")
        self.thread = threading.Thread(target=self.client.open, daemon=True)
        self.thread.start()

    def stop(self):
        print(f"[{self.hub_key}] Stopping client...")
        self.client.close()

    def update_subscribed_groups(self, subscribed_groups: List[str]):
        self.subscribed_groups = subscribed_groups
        print(f"[{self.hub_key}] Active groups now: {', '.join(subscribed_groups) or 'none'}")

    def on_connected(self, event: OnConnectedArgs):
        print(f"[{self.hub_key}] ✅ connected (ID: {event.connection_id})")
        if self.subscribed_groups:
            print(f"[{self.hub_key}] subscribed: {', '.join(self.subscribed_groups)}")

    def on_disconnected(self, event: OnDisconnectedArgs):
        print(f"[{self.hub_key}] ❌ disconnected: {event.message}")

    def on_group_message(self, event: OnGroupDataMessageArgs):
        try:
            any_message = any_pb2.Any()
            any_message.ParseFromString(event.data)
            message_type_url = any_message.type_url

            current_category = ""
            for package_name in ["classic", "state", "orderflow"]:
                separator = f"_{package_name}_"
                if separator in event.group:
                    current_category = event.group.split(separator)[-1]
                    break

            if "proto.gex" in message_type_url:
                gex_data = decompress_gex_message(any_message)
                if gex_data:
                    print(f"[{self.hub_key}] GEX: {gex_data.get('ticker')} @ {gex_data.get('spot')}")
                return

            if "proto.greek" in message_type_url:
                greek_data = decompress_greek_message(any_message, current_category)
                if greek_data:
                    print(f"[{self.hub_key}] {current_category}: {greek_data.get('ticker')}")
                return

            if "proto.orderflow" in message_type_url:
                orderflow_data = decompress_orderflow_message(any_message)
                if orderflow_data:
                    print(f"[{self.hub_key}] Orderflow: {orderflow_data.get('spot')}")
                return

            print(f"[{self.hub_key}] Unknown message type_url: {message_type_url}")
        except Exception as exc:
            print(f"[{self.hub_key}] Failed to parse message: {exc}")


def start_clients(websocket_urls: Dict[str, str], group_config: Dict[str, List[str]]) -> Dict[str, WebPubSubClientManager]:
    managers: Dict[str, WebPubSubClientManager] = {}

    for hub_key, url in websocket_urls.items():
        manager = WebPubSubClientManager(hub_key, url, group_config.get(hub_key, []))
        manager.start()
        managers[hub_key] = manager

    return managers


def update_manager_labels(managers: Dict[str, WebPubSubClientManager], group_config: Dict[str, List[str]]):
    for hub_key, manager in managers.items():
        manager.update_subscribed_groups(group_config.get(hub_key, []))


if __name__ == "__main__":
    client_managers: Dict[str, WebPubSubClientManager] = {}

    try:
        print_group_config("Initial POST groups", INITIAL_GROUP_CONFIG)
        negotiate_data = post_negotiate(API_KEY, USER_AGENT, INITIAL_GROUP_CONFIG)

        websocket_urls = negotiate_data.get("websocket_urls")
        if not websocket_urls:
            raise RuntimeError("No websocket_urls returned from negotiate.")

        print("\n--- Starting WebSocket clients ---")
        client_managers = start_clients(websocket_urls, INITIAL_GROUP_CONFIG)

        if PATCH_UPDATE_AFTER_SECONDS > 0:
            print(f"\nWaiting {PATCH_UPDATE_AFTER_SECONDS}s before PATCH group replacement example...")
            time.sleep(PATCH_UPDATE_AFTER_SECONDS)

            print_group_config("PATCH replacement groups", UPDATED_GROUP_CONFIG)
            update_result = patch_replace_groups(API_KEY, USER_AGENT, UPDATED_GROUP_CONFIG)
            print("PATCH replacement result:")
            print(json.dumps(update_result, indent=2))
            update_manager_labels(client_managers, UPDATED_GROUP_CONFIG)

        print("\nClients are running. Press Ctrl+C to stop.")
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
        if client_managers:
            print("\n--- Stopping Clients ---")
            for manager in client_managers.values():
                manager.stop()
            print("All clients stopped.")
