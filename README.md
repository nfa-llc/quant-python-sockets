# Gexbot Real-Time Data Client (Python)

This repository demonstrates the current API WebSocket flow for Quant users:

1. `POST /v2/negotiate` with the initial unprefixed groups you want.
2. Connect to the returned Azure Web PubSub hub URLs.
3. `PATCH /v2/negotiate` with a complete replacement group set to modify subscriptions without reconnecting.

## Project Structure

```
.
├── proto/                  # Source .proto definitions
├── generated_proto/        # Compiled _pb2.py files
├── main.py                 # Main client script
├── multi.py                # Quant discovery, Classic GEX, and State gamma example
├── decompression_utils.py  # Decompression helper functions
├── requirements.txt        # Python dependencies
└── README.md
```

## 1. Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 2. Compile Protobuf Definitions

```bash
mkdir -p generated_proto
touch generated_proto/__init__.py
python3 -m grpc_tools.protoc -I=proto --python_out=generated_proto --pyi_out=generated_proto proto/*.proto
```

## 3. Configuration

```bash
export GEXBOT_API_KEY="your_api_key_here"
export GEXBOT_USER_AGENT="YourClientApp/1.0"
export BASE_URL="https://api.gex.bot/v2"
```

Edit these dictionaries near the top of `main.py`:

- `INITIAL_GROUP_CONFIG` — groups sent to `POST /v2/negotiate`
- `UPDATED_GROUP_CONFIG` — replacement groups sent to `PATCH /v2/negotiate`

Group names are unprefixed:

```text
{ticker}_{package}_{category}
```

Examples:

```text
SPX_classic_gex_full
SPX_state_gamma_zero
ES_SPX_orderflow_orderflow
```

### Possible Groups

Use any supported ticker in place of `{ticker}`.

Get the current supported ticker list at: https://api.gex.bot/v2/tickers

Get package categories at:

```text
GET https://api.gex.bot/classic/categories
GET https://api.gex.bot/state/categories
GET https://api.gex.bot/orderflow/categories
```

Additional Quant tickers can be discovered at: https://api.gex.bot/tickers/quant

These supplemental Quant tickers are WebSocket/PubSub-only. They can publish realtime WebSocket groups, but they are not REST chart/history endpoints. The response separates `stocks` and `indexes`; `XSP` is returned under `indexes`.

#### Classic

```text
{ticker}_classic_gex_full
{ticker}_classic_gex_zero
{ticker}_classic_gex_one
```

#### State

```text
{ticker}_state_gex_full
{ticker}_state_gex_zero
{ticker}_state_gex_one
{ticker}_state_delta_zero
{ticker}_state_delta_one
{ticker}_state_gamma_zero
{ticker}_state_gamma_one
{ticker}_state_vanna_zero
{ticker}_state_vanna_one
{ticker}_state_charm_zero
{ticker}_state_charm_one
```

#### Orderflow

```text
{ticker}_orderflow_orderflow
```

#### Explicit Expirations

Discover all valid expiration dates within the published 90-day horizon with:

```text
GET https://api.gex.bot/v2/options/{ticker}/expiries
```

Convert returned `YYYY-MM-DD` dates to `YYYYMMDD` group suffixes:

```text
{ticker}_classic_gex_YYYYMMDD
{ticker}_state_gex_YYYYMMDD
{ticker}_state_delta_YYYYMMDD
{ticker}_state_gamma_YYYYMMDD
{ticker}_state_vanna_YYYYMMDD
{ticker}_state_charm_YYYYMMDD
{ticker}_orderflow_YYYYMMDD
```

Hub mapping for explicit expiration groups:

- `classic` for `{ticker}_classic_gex_YYYYMMDD`
- `state_gex` for `{ticker}_state_gex_YYYYMMDD`
- `state_greeks` for expiration delta/gamma/vanna/charm groups
- `orderflow` for `{ticker}_orderflow_YYYYMMDD`

Explicit expiration groups are realtime-only, publish at a lower cadence than standard groups, and use canonical option-owning tickers such as `SPX`, `NDX`, `SPY`, `QQQ`, supplemental Quant stock tickers, or the supplemental `XSP` index.

## 4. POST Negotiate

`main.py` sends:

```json
{
  "groups": [
    "SPX_state_gex_full",
    "SPX_state_gamma_zero"
  ]
}
```

The response includes all authorized hub URLs. Initial groups are auto-joined by the server; the client does not call `join_group`.

## 5. PATCH Replace Groups

After connecting, `main.py` waits briefly, then sends a full replacement request:

```json
{
  "groups": [
    { "hub": "classic", "group": "SPX_classic_gex_full" },
    { "hub": "state_gex", "group": "NDX_state_gex_full" },
    { "hub": "state_greeks_zero", "group": "SPX_state_gamma_zero" }
  ]
}
```

PATCH is a full replacement. Any active group omitted from the PATCH payload is removed.

## 6. Run

```bash
python main.py
```

## 7. Quant Discovery Example

`multi.py` demonstrates a standard Quant discovery flow:

1. Query `/v2/tickers` and select `SPX` and `NDX`.
2. Confirm `gex_full` from `/classic/categories`, plus `gex_full` and `gamma_zero` from `/state/categories`.
3. Query `https://api.gex.bot/tickers/quant` and select the first five Quant tickers.
4. Query `/v2/options/{ticker}/expiries` for those Quant tickers.
5. Subscribe every selected ticker to Classic `gex_full` and State `gex_full`.
6. Subscribe to standard State `gamma_zero` on `state_greeks_zero` for SPX/NDX and explicit `gamma_YYYYMMDD` groups on `state_greeks` for the next three valid Quant expirations.

Classic messages display only `zero_gamma`. State GEX messages display `major_positive` and `major_negative` from the payload's `major_pos_vol` and `major_neg_vol` fields. State gamma option-profile messages display `major_long_gamma` and `major_short_gamma`.

With two standard tickers, five Quant tickers, and three expirations per Quant ticker, this selects 31 of the 150 permitted groups.

```bash
python multi.py
```
