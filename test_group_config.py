"""Tests for V2 analytics and colocated spot-group configuration."""

import unittest

from group_config import (
    add_colocated_spot_groups,
    analytics_group_ticker,
    build_patch_groups,
    build_post_groups,
    count_group_memberships,
)


class GroupConfigTests(unittest.TestCase):
    """Verify the V2 group membership and negotiation payload rules."""

    def test_extracts_standard_and_futures_tickers(self) -> None:
        """Extract complete ticker names from supported analytics groups."""
        self.assertEqual(
            analytics_group_ticker("SPX_state_gamma_zero"),
            "SPX",
        )
        self.assertEqual(
            analytics_group_ticker("ES_SPX_orderflow_orderflow"),
            "ES_SPX",
        )

    def test_adds_one_spot_membership_per_ticker_and_hub(self) -> None:
        """Add one spot membership when one hub has multiple ticker categories."""
        result = add_colocated_spot_groups(
            {
                "v2_state_greeks_zero": [
                    "SPX_state_delta_zero",
                    "SPX_state_gamma_zero",
                ]
            }
        )

        self.assertEqual(
            result["v2_state_greeks_zero"],
            [
                "SPX_state_delta_zero",
                "SPX_state_gamma_zero",
                "SPX_spot",
            ],
        )

    def test_post_deduplicates_spot_names_across_hubs(self) -> None:
        """Send one unqualified spot name when the backend must colocate it."""
        config = add_colocated_spot_groups(
            {
                "v2_classic": ["SPX_classic_gex_full"],
                "v2_state_gex": ["SPX_state_gex_full"],
            }
        )

        groups = build_post_groups(config)

        self.assertEqual(groups.count("SPX_spot"), 1)
        self.assertEqual(len(groups), 3)
        self.assertEqual(count_group_memberships(config), 4)

    def test_patch_repeats_spot_membership_for_each_hub(self) -> None:
        """Qualify one ticker spot group on every related V2 hub during PATCH."""
        config = add_colocated_spot_groups(
            {
                "v2_classic": ["SPX_classic_gex_full"],
                "v2_state_gex": ["SPX_state_gex_full"],
            }
        )

        groups = build_patch_groups(config)
        spot_groups = [item for item in groups if item["group"] == "SPX_spot"]

        self.assertEqual(
            spot_groups,
            [
                {"hub": "v2_classic", "group": "SPX_spot"},
                {"hub": "v2_state_gex", "group": "SPX_spot"},
            ],
        )

    def test_rejects_non_v2_hubs(self) -> None:
        """Reject a legacy hub before it can produce an invalid V2 PATCH body."""
        with self.assertRaisesRegex(ValueError, r"v2_\*"):
            add_colocated_spot_groups({"classic": ["SPX_classic_gex_full"]})

    def test_rejects_manual_spot_configuration(self) -> None:
        """Reject manual spot entries because helpers derive them from analytics."""
        with self.assertRaisesRegex(ValueError, "added automatically"):
            add_colocated_spot_groups({"v2_classic": ["SPX_spot"]})


if __name__ == "__main__":
    unittest.main()
