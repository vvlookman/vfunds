#!/bin/sh

cargo build --release
codesign -s "DevCodeSign" --identifier "vfunds" ./target/release/vfunds
