# vfunds

Virtual Funds Backtesting and Comparison

## Service Dependencies

```sh
docker run --name aktools -p 8080:8080 -d lev1s/aktools:latest # AKTools (Open financial data)
python proxy/qmt_http.py # Access QMT (China market data), MUST run on Windows server with QMT running
```

## Develop

```sh
LOG="vfunds=debug" cargo run -- backtest -w ./example -s 2015-05-08 -e 2025-05-08 # Run example backtest
```

## Release

```sh
cargo build --release -Z unstable-options --artifact-dir ~/bin # Build binary and copy to a desitination path
```
