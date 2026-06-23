# Gexbot Real-Time Data Client (Python)

This toy client demonstrates the current API WebSocket flow for Quant users:

1. `POST /v2/negotiate` with the initial unprefixed groups you want.
2. Connect to the returned Azure Web PubSub hub URLs.
3. `PATCH /v2/negotiate` with a complete replacement group set to modify subscriptions without reconnecting.

## Project Structure

```
.
├── proto/                  # Source .proto definitions
├── generated_proto/        # Compiled _pb2.py files
├── main.py                 # Main client script
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
