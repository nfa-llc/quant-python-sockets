# Gexbot real-time data client for Python

This repository demonstrates the second-generation (V2) WebSocket flow for Quant users.

V2 sends analytics and spot as separate messages.
Each ticker needs:

- One analytics group for each selected package and category.
- One `{ticker}_spot` group on each V2 hub that carries analytics for that ticker.

Analytics and spot use separate group memberships on the same Azure Web PubSub connection.

The client flow is:

1. Send unprefixed analytics groups and one unprefixed spot group per ticker to `POST /v2/negotiate`.
2. Connect to the returned `v2_*` hub URLs.
3. Decode analytics and `proto.spot` messages separately.
4. Use the ticker and timestamp to correlate analytics with spot when the application needs one combined record.
5. Send the complete hub-qualified membership set to `PATCH /v2/negotiate` when subscriptions change.

## Project structure

```text
.
├── proto/                  # Source protobuf definitions
├── generated_proto/        # Locally generated Python protobuf files
├── main.py                 # POST and PATCH V2 example
├── multi.py                # Quant discovery and multi-ticker V2 example
├── group_config.py              # Colocated spot-group payload helpers
├── decompression_utils.py       # Analytics and spot decompression helpers
├── test_group_config.py         # Group payload tests
├── test_decompression_utils.py  # Spot wire-contract tests
├── requirements.txt             # Python dependencies
└── README.md
```

## 1. Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 2. Compile protobuf definitions

```bash
mkdir -p generated_proto
touch generated_proto/__init__.py
python3 -m grpc_tools.protoc \
  -I=proto \
  --python_out=generated_proto \
  --pyi_out=generated_proto \
  proto/*.proto
```

The generated files are not committed.
Run this command after each protobuf change.

## 3. Configuration

Set all required environment variables.

```bash
export GEXBOT_API_KEY="your_api_key_here"
export GEXBOT_USER_AGENT="YourClientApp/1.0"
export BASE_URL="https://api.gex.bot/v2"
```

The scripts also load these exact names from `quant-sockets/.env`.

Edit these analytics dictionaries near the top of `main.py`:

- `INITIAL_ANALYTICS_GROUP_CONFIG`
- `UPDATED_ANALYTICS_GROUP_CONFIG`

Use V2 hub names as dictionary keys:

```text
v2_classic
v2_state_gex
v2_state_greeks_zero
v2_state_greeks_one
v2_state_greeks
v2_orderflow
```

Configure analytics groups only.
`group_config.py` adds the required spot groups.

Analytics groups use this unprefixed shape:

```text
{ticker}_{package}_{category}
```

Spot groups use this unprefixed shape:

```text
{ticker}_spot
```

Examples:

```text
SPX_classic_gex_full
SPX_state_gamma_zero
ES_SPX_orderflow_orderflow
SPX_spot
ES_SPX_spot
```

The server adds the configured color prefix before it joins a group.
Do not add that prefix to a request.

## 4. Supported groups

Get the current ticker list from:

```text
GET https://api.gex.bot/v2/tickers
```

Get package categories from:

```text
GET https://api.gex.bot/classic/categories
GET https://api.gex.bot/state/categories
GET https://api.gex.bot/orderflow/categories
```

Get supplemental Quant tickers from:

```text
GET https://api.gex.bot/tickers/quant
```

Supplemental Quant tickers are WebSocket-only.
A V2 request must still include each matching spot-group membership.
A live spot message is available only when the publisher provides spot for that ticker.

### Classic

```text
{ticker}_classic_gex_full
{ticker}_classic_gex_zero
{ticker}_classic_gex_one
```

### State

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

### Orderflow

```text
{ticker}_orderflow_orderflow
```

### Explicit expirations

Discover valid expiration dates within the published 90-day horizon:

```text
GET https://api.gex.bot/v2/options/{ticker}/expiries
```

Convert `YYYY-MM-DD` to `YYYYMMDD` for the group suffix:

```text
{ticker}_classic_gex_YYYYMMDD
{ticker}_state_gex_YYYYMMDD
{ticker}_state_delta_YYYYMMDD
{ticker}_state_gamma_YYYYMMDD
{ticker}_state_vanna_YYYYMMDD
{ticker}_state_charm_YYYYMMDD
{ticker}_orderflow_YYYYMMDD
```

Use these V2 hubs:

- `v2_classic` for Classic GEX.
- `v2_state_gex` for State GEX.
- `v2_state_greeks` for explicit State Greek expirations.
- `v2_orderflow` for explicit Orderflow expirations.

Explicit expiration groups are real-time only.
They publish at a lower cadence than standard groups.

## 5. POST negotiation

`POST /v2/negotiate` accepts unqualified group names.
Send each analytics group and one spot name per unique ticker.

Example:

```json
{
  "groups": [
    "SPX_state_gex_full",
    "SPX_state_gamma_zero",
    "SPX_spot"
  ]
}
```

The presence of complete matching spot groups selects the V2 hubs.
The backend places `SPX_spot` on both selected V2 hubs.
Initial analytics and spot memberships are joined by the server.
Do not call Azure `joinGroup` for this flow.

The response returns URLs with V2 keys such as:

```text
v2_classic
v2_state_gex
v2_state_greeks_zero
v2_state_greeks_one
v2_state_greeks
v2_orderflow
```

## 6. PATCH replacement

`PATCH /v2/negotiate` is a full replacement.
It requires explicit hub names.
Repeat the ticker spot group on each V2 hub that has analytics for that ticker.

Example:

```json
{
  "groups": [
    { "hub": "v2_classic", "group": "SPX_classic_gex_full" },
    { "hub": "v2_classic", "group": "SPX_spot" },
    { "hub": "v2_state_gex", "group": "NDX_state_gex_full" },
    { "hub": "v2_state_gex", "group": "NDX_spot" },
    { "hub": "v2_state_greeks_zero", "group": "SPX_state_gamma_zero" },
    { "hub": "v2_state_greeks_zero", "group": "SPX_spot" }
  ]
}
```

Any membership omitted from the PATCH body is removed.
`main.py` generates the complete PATCH body from the analytics configuration.

## 7. Spot decoding

Spot arrives as a `google.protobuf.Any` envelope with this type URL:

```text
proto.spot
```

The compressed value contains `spot_price.SpotPrice`:

```text
timestamp
ticker
spot
source_timestamp_ms
```

The wire `spot` value is multiplied by 100.
`decompress_spot_message` converts it back to the decimal price.

Analytics and spot for one calculation use the same ticker and timestamp.
Do not depend on message arrival order.
Cache either message briefly if the application must combine them.

The sample scripts print spot independently from analytics.

## 8. Run the POST and PATCH example

```bash
python main.py
```

`main.py`:

- Adds matching spot groups automatically.
- Selects V2 hubs.
- Opens one connection for each returned V2 hub.
- Prints analytics and spot messages separately.
- Replaces the complete group set after the configured delay.

## 9. Run the Quant discovery example

```bash
python multi.py
```

`multi.py`:

1. Selects `SPX` and `NDX` from `/v2/tickers`.
2. Confirms the required Classic and State categories.
3. Selects five supplemental Quant tickers.
4. Gets the next three valid expirations for each supplemental ticker.
5. Builds Classic, State GEX, standard gamma, and explicit gamma analytics groups.
6. Adds one matching spot membership per ticker and V2 hub.
7. Negotiates and opens the selected V2 connections.
8. Prints analytics and spot messages separately.

With two standard tickers, five supplemental tickers, and three expirations per supplemental ticker, the example selects:

- 31 analytics memberships.
- 21 colocated spot memberships.
- 52 total memberships from the Quant limit of 150.

The POST body contains fewer spot names because it sends each ticker spot name once.
The backend expands that name onto each applicable V2 hub.

## 10. Run tests

Compile the Protocol Buffer definitions first.
Then run:

```bash
python -m unittest test_group_config.py test_decompression_utils.py
```
