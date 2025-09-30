# vfunds

Virtual Funds Backtesting and Comparison

## Develop

```sh
LOG="vfunds=debug" cargo run -- backtest -w ./example -s 2015-05-08 -e 2025-05-08 # Run example backtest
```

## Release

```sh
cargo build --release -Z unstable-options --artifact-dir ~/bin # Build binary and copy to a desitination path
```
