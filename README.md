# vfunds

Virtual Funds Backtesting and Comparison

## Service Dependencies

```sh
docker run --name aktools -p 8080:8080 -d lev1s/aktools:latest # AKTools (Open financial data)
python proxy/qmt_http.py # Access QMT (China market data), MUST run on Windows server with QMT running
```

## Run

```sh
vfunds config show # Show configurations
vfunds config set qmt_api http://192.168.0.222:9000 # Set configuration

vfunds list -w ~/vfunds/example # List all virtual funds
vfunds backtest -w ~/vfunds/example -o ~/vfunds/output -s 2020-11-08 # Run backtest
vfunds result -o ~/vfunds/output -g # Show backtest result with GUI chart
CACHE_NO_EXPIRE=true vfunds backtest -s 2020-11-08 @permanent -S -p # Run cross-validation backtests, ignoring cache expiration
```

## Develop

```sh
LOG="vfunds=debug" CACHE_NO_EXPIRE=true cargo run -- backtest -w ./example -s 2015-05-08 -e 2025-05-08 @permanent
```

## Release

```sh
cargo build --release -Z unstable-options --artifact-dir ~/bin # Build binary and copy to a desitination path
```
