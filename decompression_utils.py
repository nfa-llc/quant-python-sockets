import json
import zstandard
from typing import Dict
from google.protobuf import any_pb2

# --- Import generated proto files ---
# Ensure the 'generated_proto' directory is a package (has __init__.py)
import generated_proto.gex_pb2 as gex_pb2
import generated_proto.option_profile_pb2 as option_profile_pb2
import generated_proto.orderflow_pb2 as orderflow_pb2
# ------------------------------------

# --- Zstandard Decompressor ---
# Create a single reusable decompression context
DCTX = zstandard.ZstdDecompressor()
# ------------------------------


def decompress_gex_message(any_message: any_pb2.Any) -> Dict:
    """
    Decompresses and decodes a ZSTD-compressed Gex message from a google.protobuf.Any.
    """
    # 1. Decompress the raw bytes
    compressed_bytes = any_message.value
    with DCTX.stream_reader(compressed_bytes) as reader:
        decompressed_bytes = reader.read()

    # 2. Decode the Gex Protobuf data
    decoded_proto = gex_pb2.Gex()
    decoded_proto.ParseFromString(decompressed_bytes)

    # 3. Convert Protobuf Gex to Python Dict, applying inverse multiplications
    classic_gex = {
        "timestamp": decoded_proto.timestamp,
        "ticker": decoded_proto.ticker,
        "min_dte": decoded_proto.min_dte or 0,
        "sec_min_dte": decoded_proto.sec_min_dte or 1,
        "spot": (decoded_proto.spot or 0) / 100.0,
        "zero_gamma": (decoded_proto.zero_gamma or 0) / 100.0,
        "major_pos_vol": (decoded_proto.major_pos_vol or 0) / 100.0,
        "major_pos_oi": (decoded_proto.major_pos_oi or 0) / 100.0,
        "major_neg_vol": (decoded_proto.major_neg_vol or 0) / 100.0,
        "major_neg_oi": (decoded_proto.major_neg_oi or 0) / 100.0,
        "strikes": [
            [
                (s.strike_price or 0) / 100.0,
                (s.value_1 or 0) / 100.0,
                (s.value_2 or 0) / 100.0,
                # Assumes s.priors is a message with a 'values' field
                [v /
                    100.0 for v in s.priors.values] if s.HasField("priors") else []
            ]
            for s in decoded_proto.strikes
        ],
        "sum_gex_vol": (decoded_proto.sum_gex_vol or 0) / 1000.0,
        "sum_gex_oi": (decoded_proto.sum_gex_oi or 0) / 1000.0,
        "delta_risk_reversal": (decoded_proto.delta_risk_reversal or 0) / 1000.0,
        "max_priors": [
            [
                (t.first_value or 0) / 100.0,
                (t.second_value or 0) / 1000.0
            ]
            # Assumes max_priors is a message with a 'tuples' field
            for t in decoded_proto.max_priors.tuples
        ] if decoded_proto.HasField("max_priors") else [],
    }

    return classic_gex


def decompress_greek_message(any_message: any_pb2.Any, current_category: str) -> Dict:
    """
    Decompresses and decodes a ZSTD-compressed message.
    Conditionally deserializes as JSON or Protobuf based on the category.
    """
    # 1. Decompress the raw bytes
    compressed_bytes = any_message.value
    with DCTX.stream_reader(compressed_bytes) as reader:
        decompressed_bytes = reader.read()

    # 2. Conditionally deserialize
    if current_category in ('volume_zero', 'volume_one'):
        # --- JSON Deserialization Path ---
        json_string = decompressed_bytes.decode('utf-8')
        raw_profile = json.loads(json_string)

        live_contracts = {
            "timestamp": raw_profile.get("timestamp"),
            "ticker": raw_profile.get("ticker"),
            "spot": raw_profile.get("spot", 0),
            "min_dte": raw_profile.get("min_dte", 0),
            "sec_min_dte": raw_profile.get("sec_min_dte", 1),
            "major_positive": raw_profile.get("major_call_gamma", 0),
            "major_negative": raw_profile.get("major_put_gamma", 0),
            "major_long_gamma": raw_profile.get("major_long_gamma", 0),
            "major_short_gamma": raw_profile.get("major_short_gamma", 0),
            "mini_contracts": [
                [
                    mc[0],  # strike
                    mc[1],  # call_ivol
                    mc[2],  # put_ivol
                    mc[3],  # call_cvolume
                    mc[4] or [],  # call_cvolume_priors
                    mc[5] or 0,  # put_cvolume
                    mc[6] or [],  # put_cvolume_priors
                ]
                for mc in raw_profile.get("mini_contracts", [])
            ],
        }
        return live_contracts

    else:
        # --- Protobuf Deserialization Path ---
        decoded_proto = option_profile_pb2.OptionProfile()
        decoded_proto.ParseFromString(decompressed_bytes)

        live_contracts = {
            "timestamp": decoded_proto.timestamp,
            "ticker": decoded_proto.ticker,
            "spot": (decoded_proto.spot or 0) / 100.0,
            "min_dte": decoded_proto.min_dte or 0,
            "sec_min_dte": decoded_proto.sec_min_dte or 1,
            "major_positive": (decoded_proto.major_call_gamma or 0) / 100.0,
            "major_negative": (decoded_proto.major_put_gamma or 0) / 100.0,
            "major_long_gamma": (decoded_proto.major_long_gamma or 0) / 100.0,
            "major_short_gamma": (decoded_proto.major_short_gamma or 0) / 100.0,
            "mini_contracts": [
                [
                    (mc.strike or 0) / 100.0,
                    (mc.call_ivol or 0) / 1000.0,
                    (mc.put_ivol or 0) / 1000.0,
                    (mc.call_cvolume or 0) / 100.0,
                    # Assumes call_cvolume_priors is a repeated field
                    [(v or 0) / 100.0 for v in mc.call_cvolume_priors],
                    mc.put_cvolume or 0,
                    # Assumes put_cvolume_priors is a message with 'values'
                    [v for v in mc.put_cvolume_priors.values] if mc.HasField(
                        "put_cvolume_priors") else [],
                ]
                for mc in decoded_proto.mini_contracts
            ],
        }
        return live_contracts


def decompress_orderflow_message(any_message: any_pb2.Any) -> Dict:
    compressed_bytes = any_message.value
    with DCTX.stream_reader(compressed_bytes) as reader:
        decompressed_bytes = reader.read()

    p = orderflow_pb2.Orderflow()
    p.ParseFromString(decompressed_bytes)

    return {
        "timestamp": p.timestamp,
        "ticker": p.ticker,
        "spot": (p.spot or 0) / 100.0,
        "zero_major_long_gamma":  (p.zero_major_long_gamma or 0) / 100.0,
        "zero_major_short_gamma": (p.zero_major_short_gamma or 0) / 100.0,
        "one_major_long_gamma":   (p.one_major_long_gamma or 0) / 100.0,
        "one_major_short_gamma":  (p.one_major_short_gamma or 0) / 100.0,
        "zero_major_call_gamma":  (p.zero_major_call_gamma or 0) / 100.0,
        "zero_major_put_gamma":   (p.zero_major_put_gamma or 0) / 100.0,
        "one_major_call_gamma":   (p.one_major_call_gamma or 0) / 100.0,
        "one_major_put_gamma":    (p.one_major_put_gamma or 0) / 100.0,

        # state — all x100 on the wire
        "zero_convexity_ratio": p.zero_convexity_ratio / 100.0,
        "one_convexity_ratio":  p.one_convexity_ratio / 100.0,
        "zero_gex_ratio":       p.zero_gex_ratio / 100.0,
        "one_gex_ratio":        p.one_gex_ratio / 100.0,
        "zero_net_vanna":       p.zero_net_vanna / 100.0,
        "one_net_vanna":        p.one_net_vanna / 100.0,
        "zero_net_charm":       p.zero_net_charm / 100.0,
        "one_net_charm":        p.one_net_charm / 100.0,
        "zero_agg_total_dex":   p.zero_agg_total_dex / 100.0,
        "one_agg_total_dex":    p.one_agg_total_dex / 100.0,
        "zero_agg_call_dex":    p.zero_agg_call_dex / 100.0,
        "one_agg_call_dex":     p.one_agg_call_dex / 100.0,
        "zero_agg_put_dex":     p.zero_agg_put_dex / 100.0,
        "one_agg_put_dex":      p.one_agg_put_dex / 100.0,
        "zero_net_total_dex":   p.zero_net_total_dex / 100.0,
        "one_net_total_dex":    p.one_net_total_dex / 100.0,
        "zero_net_call_dex":    p.zero_net_call_dex / 100.0,
        "one_net_call_dex":     p.one_net_call_dex / 100.0,
        "zero_net_put_dex":     p.zero_net_put_dex / 100.0,
        "one_net_put_dex":      p.one_net_put_dex / 100.0,

        # orderflow — also x100 on the wire
        "dex_orderflow":           p.dex_orderflow / 100.0,
        "gex_orderflow":           p.gex_orderflow / 100.0,
        "convexity_orderflow":     p.convexity_orderflow / 100.0,
        "one_dex_orderflow":       p.one_dex_orderflow / 100.0,
        "one_gex_orderflow":       p.one_gex_orderflow / 100.0,
        "one_convexity_orderflow": p.one_convexity_orderflow / 100.0,
    }
