"""Tests for separate V2 spot-message decompression."""

import unittest

import zstandard
from google.protobuf import any_pb2

import generated_proto.spot_price_pb2 as spot_price_pb2
from decompression_utils import decompress_spot_message


class SpotDecompressionTests(unittest.TestCase):
    """Verify the public V2 spot Protocol Buffer wire contract."""

    def test_decompresses_scaled_spot_and_source_timestamp(self) -> None:
        """Decode all spot fields and reverse the wire price scaling."""
        message = spot_price_pb2.SpotPrice(
            timestamp=1_754_000_000,
            ticker="SPX",
            spot=642_125,
            source_timestamp_ms=1_754_000_000_123,
        )
        envelope = any_pb2.Any(
            type_url="proto.spot",
            value=zstandard.ZstdCompressor().compress(message.SerializeToString()),
        )

        decoded = decompress_spot_message(envelope)

        self.assertEqual(
            decoded,
            {
                "timestamp": 1_754_000_000,
                "ticker": "SPX",
                "spot": 6_421.25,
                "source_timestamp_ms": 1_754_000_000_123,
            },
        )

    def test_returns_none_when_source_timestamp_is_absent(self) -> None:
        """Keep the optional source timestamp absent when it is not on the wire."""
        message = spot_price_pb2.SpotPrice(
            timestamp=1_754_000_000,
            ticker="SPX",
            spot=642_125,
        )
        envelope = any_pb2.Any(
            type_url="proto.spot",
            value=zstandard.ZstdCompressor().compress(message.SerializeToString()),
        )

        decoded = decompress_spot_message(envelope)

        self.assertIsNone(decoded["source_timestamp_ms"])


if __name__ == "__main__":
    unittest.main()
