# FX Edge Lab

`fx-edge-lab` is a futures-vs-spot basis arbitrage detector.

This is not Fair Value Gap.
This is not single-chart price action.
This is cross-instrument basis arbitrage between CME futures and forex spot.

What it does:

- connects to two live price feeds for the same market
- reads spot prices from MetaTrader 5 through the `MetaTrader5` Python package
- reads futures prices from either Polygon.io WebSocket or Interactive Brokers TWS API
- normalizes futures and spot into the same quote convention
- calculates the live futures-vs-spot gap every time quotes update
- fires alerts when the gap exceeds a per-pair threshold
- optionally sends a Telegram message
- optionally auto-executes the spot side on MT5
- stores every evaluated gap plus alert outcomes in SQLite

Pairs supported by the default config:

- `EURUSD` vs `6E`
- `GBPUSD` vs `6B`
- `XAUUSD` vs `GC`
- `USDJPY` vs `6J`

## Important Notes

- `USDJPY` needs quote normalization because CME `6J` is quoted in the inverse convention relative to OTC spot.
- The default monitor uses mid prices for gap detection, then uses live MT5 bid or ask for execution.
- To avoid alert spam, the engine applies a per-pair cooldown before emitting the same side again.
- Polygon futures WebSocket support is still documented as beta, so treat it as a vendor dependency risk.

## Files

- `src/fx_edge_lab/settings.py`: config loading and defaults
- `src/fx_edge_lab/engine.py`: basis calculation, alert logic, and outcome tracking
- `src/fx_edge_lab/service.py`: live monitor orchestration
- `src/fx_edge_lab/connectors/mt5.py`: MT5 spot feed and optional order execution
- `src/fx_edge_lab/connectors/polygon.py`: Polygon futures WebSocket feed
- `src/fx_edge_lab/connectors/ib.py`: IB futures market data feed
- `src/fx_edge_lab/storage.py`: SQLite schema and persistence
- `src/fx_edge_lab/replay.py`: CSV replay for dry runs and backtests
- `config.example.json`: editable example config

## Installation

Core package:

```powershell
python -m pip install -e .
```

Live Polygon monitor:

```powershell
python -m pip install websockets
python -m pip install MetaTrader5
```

Live IB monitor:

```powershell
python -m pip install -e .[fx-live]
```

## Configuration

Copy and edit `config.example.json`.

The most important fields are:

- `futures_provider.kind`: `polygon` or `ib`
- `futures_provider.api_key`: Polygon API key when using Polygon
- `mt5`: terminal login or path settings if your MT5 instance needs them
- `telegram`: bot token and chat id if you want push alerts
- `pairs`: broker spot symbol, active futures symbol, pip size, threshold, and normalization mode

You need to update the futures symbols to the active contract names your feed expects.

## Run

Start the live monitor:

```powershell
python .\run_fx_edge_lab.py monitor --config .\config.example.json
```

Run for a short session while testing:

```powershell
python .\run_fx_edge_lab.py monitor --config .\config.example.json --run-seconds 60
```

Replay sample data through the same engine:

```powershell
python .\run_fx_edge_lab.py replay-csv .\data\sample_basis_ticks.csv --config .\config.example.json
```

Print the merged runtime config:

```powershell
python .\run_fx_edge_lab.py print-config --config .\config.example.json
```

## Replay CSV Schema

Replay mode expects:

```text
timestamp,pair,spot_bid,spot_ask,futures_bid,futures_ask
```

`pair` must match a configured pair name such as `EURUSD` or `USDJPY`.

## Alert Output

Each alert includes:

- pair
- direction: `BUY` or `SELL` spot
- raw futures price
- normalized futures-equivalent price
- spot price
- gap in pips
- timestamp

When execution is enabled on MT5:

- lot size defaults to `0.01`
- `SL = gap * 1.5`
- `TP = gap closes`

## SQLite

The monitor creates:

- `gap_ticks`: every evaluated futures-vs-spot gap
- `alerts`: alert lifecycle, execution, and theoretical closure

This gives you a clean SQLite trail for later review and replay analysis.

## Sources

- [MetaTrader 5 Python integration](https://www.mql5.com/en/docs/python_metatrader5)
- [MetaTrader 5 `order_send`](https://www.mql5.com/en/docs/python_metatrader5/mt5ordersend_py)
- [IBKR TWS API docs](https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/)
- [Polygon futures WebSocket quotes](https://polygon.io/docs/websocket/futures/quotes)
- [Polygon forex WebSocket quotes](https://polygon.io/docs/websocket/forex/quotes)
- [CME reconciling spot and futures conventions](https://www.cmegroup.com/education/whitepapers/reconciling-fx-spot-futures-prices.html)
- [CME FX monthly contract specifications](https://www.cmegroup.com/education/fx-monthly-futures-contract-specifications.html)

## Crypto Research Stack

The project also includes a separate crypto microstructure research mode built around free public exchange feeds.

What it captures:

- Binance spot top-of-book depth snapshots and public trades
- Bybit linear perpetual top-of-book depth updates and public trades
- Bybit funding snapshots and recent funding history
- Binance futures funding snapshots and recent funding history
- Binance 5-minute open interest history snapshots
- Cross-venue basis between Binance spot and Bybit perpetual
- Basis-plus-funding spread signals with adaptive entry thresholds
- Paper spread positions: long Binance spot and short Bybit perp
- Live signal quality scoring based on current edge versus modeled fees
- Multi-strategy research leaderboard for basis carry, funding flips, funding divergence, and liquidation snap-back

New files:

- `src/fx_edge_lab/crypto_models.py`
- `src/fx_edge_lab/crypto_settings.py`
- `src/fx_edge_lab/crypto_storage.py`
- `src/fx_edge_lab/crypto_engine.py`
- `src/fx_edge_lab/crypto_service.py`
- `src/fx_edge_lab/crypto_analysis.py`
- `src/fx_edge_lab/connectors/binance_public.py`
- `src/fx_edge_lab/connectors/bybit_public.py`
- `crypto.example.json`

Run a short public-data capture:

```powershell
python .\run_fx_edge_lab.py crypto-capture --config .\crypto.example.json --run-seconds 30
```

Summarize the research database:

```powershell
python .\run_fx_edge_lab.py crypto-analyze --config .\crypto.example.json
```

Print the merged crypto config:

```powershell
python .\run_fx_edge_lab.py crypto-print-config --config .\crypto.example.json
```

Keep the collector running continuously:

```powershell
python .\run_fx_edge_lab.py crypto-capture --config .\crypto.example.json
```

Open the local dashboard in a second terminal:

```powershell
python .\run_fx_edge_lab.py crypto-dashboard --config .\crypto.example.json
```

Then open:

```text
http://127.0.0.1:8765
```

## Windows Server Setup

For a fresh Windows Server host, run:

```bat
setup.bat
```

What it does:

- installs Python 3.13 if needed
- creates `.venv`
- installs the package in editable mode
- copies `crypto.example.json` to `crypto.local.json` if missing
- copies `config.example.json` to `config.local.json` if missing

Optional modes:

- `setup.bat --start`: install, then launch `crypto-capture` and `crypto-dashboard` in two new windows
- `setup.bat --public-dashboard`: same as `--start`, but binds the dashboard to `0.0.0.0:8765` and opens the Windows firewall port

After setup, the default public-data crypto lab runs with:

```bat
.venv\Scripts\python.exe run_fx_edge_lab.py crypto-capture --config crypto.local.json
.venv\Scripts\python.exe run_fx_edge_lab.py crypto-dashboard --config crypto.local.json
```

How to use it day to day:

- Terminal 1 runs `crypto-capture` and stays open to keep collecting live Binance and Bybit data.
- Terminal 2 runs `crypto-dashboard` and serves a local page backed by the same SQLite database.
- Your browser shows live counts, the live signal quality board, latest basis, open spread PnL, closed spread performance, a realized net equity curve, recent spread signals, and a per-trade spread blotter.
- The dashboard also shows a strategy leaderboard that ranks multiple research modules on the same data and fee assumptions.
- `crypto-analyze` gives you a text summary whenever you want a quick report from the same database.
- If you want a fresh session, point `database_path` in `crypto.example.json` to a new SQLite file.

What the paper simulator means here:

- it treats each signal as a paper spread: long spot and short perp
- entries require basis, funding, and 3-minute basis momentum to align
- exits happen on basis convergence, funding flip, or the max hold timer
- spread PnL now shows both gross and estimated net PnL
- estimated net PnL uses `fee_preset`, `exit_mode`, and `exit_slippage_bps` from `crypto.example.json` as aggregate spread-trade friction assumptions
- supported fee presets are `custom`, `binance_spot_regular`, `binance_spot_regular_bnb`, `bybit_spot_vip0`, and `bybit_linear_vip0`
- supported exit modes are `mid`, `maker`, and `taker`
- you can still override `maker_entry_fee_bps` or `exit_fee_bps` manually if your actual exchange tier differs from the preset
- the paper model is still queue-unaware, so treat the results as optimistic until queue position and separate spot/perp fee legs are modeled

Crypto sources:

- [Binance spot WebSocket streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams)
- [Binance funding history](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History)
- [Bybit public orderbook stream](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook)
- [Bybit public trade stream](https://bybit-exchange.github.io/docs/v5/websocket/public/trade)
- [Bybit tickers](https://bybit-exchange.github.io/docs/v5/market/tickers)
- [Bybit funding history](https://bybit-exchange.github.io/docs/v5/market/history-fund-rate)
