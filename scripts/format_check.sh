#!/bin/bash

# Simple script to check code formatting locally before pushing

set -e

echo "Checking code formatting..."
echo ""

echo "1. Checking black formatting..."
black --check .
echo "Black check passed!"
echo ""

echo "2. Checking isort import sorting..."
isort --check-only .
echo "Isort check passed!"
echo ""

echo "All format checks passed!"

