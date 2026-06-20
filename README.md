# Gexbot Real-Time Data Client (Python)

This toy client demonstrates how API Quant users connect to Gexbot real-time sockets:

1. Build the unprefixed websocket groups you want, e.g. `ES_SPX_classic_gex_full`.
2. `POST /v2/negotiate` with `{"groups": [...]}`.
3. Open the returned Azure Web PubSub URLs. The negotiated token already includes the requested groups, so the client does **not** call `join_group`.

## Project Structure

```
.
├── proto/                  # Source .proto definitions (e.g., gex.proto)
├── generated_proto/        # Compiled _pb2.py files
├── main.py                 # Main client script
├── decompression_utils.py  # Decompression helper functions
├── requirements.txt        # Python dependencies
├── .gitignore
└── README.md
```

---

## 1. Setup and Installation

1. **Clone Repository**
   Clone this repository to your local machine.

2. **Create Virtual Environment**
   It's highly recommended to use a virtual environment.

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

---

## 2. Compile Protobuf Definitions

This client reads data from Protobuf messages. Compile the source `.proto` files in `proto/` into Python modules.

```bash
mkdir -p generated_proto
touch generated_proto/__init__.py
python3 -m grpc_tools.protoc -I=proto --python_out=generated_proto --pyi_out=generated_proto proto/*.proto
```

---

## 3. Configuration

Set the required environment variables:

```bash
export GEXBOT_API_KEY="your_api_key_here"
export GEXBOT_USER_AGENT="YourClientApp/1.0"
```

Then open `main.py` and edit the **USER SELECTION** section:

- `ACTIVE_TICKERS`
- `ACTIVE_CLASSIC_CATEGORIES`
- `ACTIVE_STATE_GEX_CATEGORIES`
- `ACTIVE_STATE_GREEKS_ZERO_CATEGORIES`
- `ACTIVE_STATE_GREEKS_ONE_CATEGORIES`
- `ACTIVE_ORDERFLOW_CATEGORIES`

Groups sent to negotiate are as follows:

```text
{ticker}_{package}_{category}
```

Examples:

```text
SPX_classic_gex_full
ES_SPX_state_gamma_zero
SPX_orderflow_orderflow
```

---

## 4. Run the Client

```bash
python main.py
```
