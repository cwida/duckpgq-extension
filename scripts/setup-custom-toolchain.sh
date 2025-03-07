#!/bin/bash

# This is an example script that can be used to install additional toolchain dependencies. Feel free to remove this script
# if no additional toolchains are required

# To use this script in the GitHub Actions setup, override the `configure_ci` target in your Makefile
# and have it call this script.
# Example: `bash ./scripts/setup-custom-toolchain.sh`

# note that the $DUCKDB_PLATFORM environment variable can be used to discern between the platforms
echo "This is the sample custom toolchain script running for architecture '$DUCKDB_PLATFORM' for the quack extension."

