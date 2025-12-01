# vfunds

Virtual Funds Backtesting and Comparison

## Dependent Services

```sh
python proxy/qmt_http.py # Access QMT (China market data), MUST run on Windows server with QMT running
```

## Run

```sh
vfunds config show # Show configurations
vfunds config set qmt_api http://192.168.0.222:9000 # Set where to access QMT
vfunds config set tushare_token xxx # Get Tushare token first

vfunds list -w ~/vfunds/example # List all virtual funds
vfunds backtest -w ~/vfunds/example -o ~/vfunds/output -s 2018-01-01 # Run backtest
vfunds result -o ~/vfunds/output -g # Show backtest result with GUI chart
CACHE_NO_EXPIRE=true vfunds backtest -s 2018-01-01 @permanent -S -p # Run cross-validation backtests, ignoring cache expiration
```

## Develop

```sh
LOG="vfunds=debug" CACHE_NO_EXPIRE=true cargo run -- backtest -w ./example -s 2018-01-01 @permanent
```

## Release

```sh
cargo build --release -Z unstable-options --artifact-dir ~/bin # Build binary and copy to a desitination path
```
