#!/bin/bash

# E-Commerce Data Pipeline Runner
# This script sets up the environment and runs the pipeline

set -e  # Exit on error

echo "=========================================="
echo "E-Commerce Data Pipeline"
echo "=========================================="
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies if needed
if [ ! -f "venv/.installed" ]; then
    echo "Installing dependencies..."
    pip install -r requirements.txt
    touch venv/.installed
fi

# Check if data file exists
if [ ! -f "data/ecommerce_sales.csv" ]; then
    echo ""
    echo "No input data found. Generating sample data..."
    python -m src.utils.data_generator
    echo ""
fi

# Run the pipeline
echo "Starting pipeline execution..."
echo ""
python -m src.pipeline

echo ""
echo "=========================================="
echo "Pipeline completed!"
echo "=========================================="
echo ""
echo "Output files:"
echo "  - data/output/monthly_sales_summary.csv"
echo "  - data/output/top_products.csv"
echo "  - data/output/anomaly_records.csv"
echo "  - data/output/data_quality_report.json"
echo ""
echo "Logs:"
echo "  - logs/pipeline.log"
echo ""

