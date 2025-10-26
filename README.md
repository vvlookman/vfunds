# vfunds

Virtual Funds Backtesting and Comparison

## Service Dependencies

```sh
docker run --name aktools -p 8080:8080 -d lev1s/aktools:latest # AKTools (Open financial data)
python proxy/qmt_http.py # Access QMT (China market data), MUST run on Windows server with QMT running
```

## Run

```sh
vfunds backtest -w ~/vfunds/example -o ~/vfunds/output -s 2019-08-08 # Run example backtest
vfunds list -w ~/vfunds/example # List virtual funds
vfunds show -o ~/vfunds/output # Show backtest results
AKTOOLS_API="http://127.0.0.1:8080" QMT_API="http://192.168.0.222:9000" vfunds backtest -w ./example -s 2019-08-08 # Set dependencies service address
```

## Develop

```sh
LOG="vfunds=debug" cargo run -- backtest -w ./example -s 2015-05-08 -e 2025-05-08 @permanent
```

## Release

```sh
cargo build --release -Z unstable-options --artifact-dir ~/bin # Build binary and copy to a desitination path
```
