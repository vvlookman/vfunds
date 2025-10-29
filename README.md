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
vfunds backtest -w ~/vfunds/example -o ~/vfunds/output -s 2019-08-08 # Run backtest
vfunds result -o ~/vfunds/output -g # Show backtest result with GUI chart
```

## Develop

```sh
LOG="vfunds=debug" cargo run -- backtest -w ./example -s 2015-05-08 -e 2025-05-08 @permanent
```

## Release

```sh
cargo build --release -Z unstable-options --artifact-dir ~/bin # Build binary and copy to a desitination path
```
