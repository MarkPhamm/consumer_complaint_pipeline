#!/bin/bash

# Simple script to auto-fix code formatting

set -e

echo "Auto-fixing code formatting..."
echo ""

echo "1. Running black..."
black .
echo "Black formatting applied!"
echo ""

echo "2. Running isort..."
isort .
echo "Isort import sorting applied!"
echo ""

echo "All formatting fixed!"

