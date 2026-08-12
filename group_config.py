"""Build V2 analytics and colocated spot-group negotiation payloads."""

from typing import Dict, List

V2_HUB_PREFIX = "v2_"
ANALYTICS_GROUP_MARKERS = ("_classic_", "_state_", "_orderflow_")


def analytics_group_ticker(group: str) -> str:
    """Return the ticker from one unprefixed analytics group."""
    normalized = group.strip()
    if normalized.lower().endswith("_spot"):
        raise ValueError(
            "Configure analytics groups only; spot groups are added automatically."
        )

    for marker in ANALYTICS_GROUP_MARKERS:
        marker_index = normalized.lower().find(marker)
        if marker_index > 0:
            return normalized[:marker_index]

    raise ValueError(f"Cannot derive a ticker from analytics group: {group}")


def add_colocated_spot_groups(
    analytics_group_config: Dict[str, List[str]],
) -> Dict[str, List[str]]:
    """Add one matching ticker spot group to each V2 analytics hub."""
    group_config: Dict[str, List[str]] = {}

    for hub, analytics_groups in analytics_group_config.items():
        if not hub.startswith(V2_HUB_PREFIX):
            raise ValueError(f"V2 group configuration requires a v2_* hub: {hub}")

        tickers = list(
            dict.fromkeys(analytics_group_ticker(group) for group in analytics_groups)
        )
        group_config[hub] = [
            *analytics_groups,
            *(f"{ticker}_spot" for ticker in tickers),
        ]

    return group_config


def build_post_groups(group_config: Dict[str, List[str]]) -> List[str]:
    """Build the unqualified POST group list with duplicate spot names removed."""
    return list(
        dict.fromkeys(group for groups in group_config.values() for group in groups)
    )


def build_patch_groups(group_config: Dict[str, List[str]]) -> List[Dict[str, str]]:
    """Build the hub-qualified full replacement list for PATCH negotiation."""
    return [
        {"hub": hub, "group": group}
        for hub, groups in group_config.items()
        for group in groups
    ]


def count_group_memberships(group_config: Dict[str, List[str]]) -> int:
    """Count the final hub-and-group memberships enforced by the backend limit."""
    return sum(len(groups) for groups in group_config.values())
