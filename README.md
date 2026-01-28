# E-Commerce Data Pipeline

A scalable, single‑machine data pipeline that ingests, cleans, and aggregates large e‑commerce sales datasets (100M+ rows) into analytics‑ready reports with explicit data‑quality tracking.

## 🚀 Quick Start

```bash
# 1. Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate 

# 2. Install dependencies
pip install -r requirements.txt

# 3. Generate sample data (creates 1M row dataset with realistic quality issues)
#    Data spans the last 3 years up to today
python3 -m src.utils.data_generator

# 4. Run the pipeline
python3 -m src.pipeline

# 5. View results
ls -lh data/output/
# → monthly_sales_summary.csv
# → top_products.csv
# → anomaly_records.csv
# → data_quality_report.json
```

## ✅ Requirements Met

This pipeline fulfills all specified requirements:

### 1. Ingestion ✓
- **Requirement**: Load ~100M rows from CSV using chunked reading and memory-efficient structures
- **Implementation**: `src/ingestion/csv_reader.py` - ChunkedCSVReader class
  - Configurable chunk size (default: 100,000 rows)
  - Never loads entire dataset into memory
  - Handles files larger than available RAM

### 2. Cleansing & Normalization ✓
- **Requirement**: Identify data quality issues and apply custom cleaning rules
- **Implementation**: `src/cleansing/data_cleaner.py` - DataCleaner class
  - Handles 8+ types of data quality issues (see below)
  - All cleaning rules documented and configurable
  - Comprehensive quality metrics tracking

### 3. Transformations ✓
- **Requirement**: Produce analytical datasets
- **Implementation**: `src/transformation/aggregator.py` - DataAggregator class

**Exact deliverables as specified:**
1. ✅ **monthly_sales_summary.csv** - Revenue, quantity, avg discount by month
2. ✅ **top_products.csv** - Top 10 products by revenue and units sold
3. ✅ **anomaly_records.csv** - Top 5 highest-revenue records

**Bonus:** `data_quality_report.json` - Comprehensive quality metrics

## 📊 Data Quality Issues Handled

The pipeline automatically detects and handles:

- ✅ **Duplicate orders**: Removes duplicate `order_id` entries
- ✅ **Invalid quantities**: Converts strings, removes negative values (zero allowed by default)
- ✅ **Invalid prices**: Validates price ranges (0.01 - 100,000)
- ✅ **Invalid discounts**: Validates discount ranges (0 - 100%)
- ✅ **Date parsing**: Handles multiple date formats
- ✅ **Region normalization**: Standardizes region names (typos, variants)
- ✅ **Category normalization**: Standardizes category names
- ✅ **Missing values**: Removes rows with critical missing data
- ✅ **Revenue calculation**: Recalculates from quantity × price × (1 - discount)

## 📁 Project Structure

```
.
├── src/
│   ├── config/
│   │   └── settings.py          # Configuration settings
│   ├── ingestion/
│   │   └── csv_reader.py        # Chunked CSV reader
│   ├── cleansing/
│   │   └── data_cleaner.py      # Data cleaning logic
│   ├── transformation/
│   │   └── aggregator.py        # Data aggregation
│   ├── utils/
│   │   └── data_generator.py    # Sample data generator
│   └── pipeline.py              # Main pipeline orchestrator
├── tests/
│   ├── test_pipeline.py         # E2E pipeline tests
│   └── conftest.py              # Test configuration
├── data/
│   └── output/                  # Generated reports
├── logs/
│   └── pipeline.log             # Execution logs
├── requirements.txt             # Python dependencies
├── setup.py                     # Package setup
└── README.md                    # This file
```

## 📈 Output Files

The pipeline generates four output files in `data/output/`:

### 1. Monthly Sales Summary (`monthly_sales_summary.csv`)

Aggregated sales metrics by month:

| year_month | total_revenue | total_quantity | avg_discount |
|------------|---------------|----------------|--------------|
| 2024-01    | 1,234,567.89  | 45,678         | 12.34        |
| 2024-02    | 1,345,678.90  | 48,901         | 11.89        |

### 2. Top Products (`top_products.csv`)

Top 10 products by revenue and units sold:

| product_name | total_revenue | total_units_sold | rank_by |
|--------------|---------------|------------------|---------|
| Laptop Pro   | 567,890.12    | 1,234            | revenue |
| USB Cable    | 123,456.78    | 45,678           | units   |

### 3. Anomaly Records (`anomaly_records.csv`)

Top 5 highest-revenue transactions:

| order_id | product_name | revenue   | ... |
|----------|--------------|-----------|-----|
| ORD12345 | Laptop Pro   | 45,678.90 | ... |

### 4. Data Quality Report (`data_quality_report.json`)

Comprehensive quality metrics (automatically converts numpy types to JSON-serializable format):

```json
{
  "total_rows_processed": 100000000,
  "total_rows_cleaned": 95234567,
  "rows_removed": 4765433,
  "data_quality_score": 95.23,
  "duplicate_orders": 1234567,
  "invalid_quantity": 234567,
  "invalid_price": 123456,
  "invalid_discount": 89012,
  "invalid_date": 45678,
  "missing_values": 234567
}
```

## ⚙️ Configuration

Edit `src/config/settings.py` to customize:

- **Chunk size**: `CHUNK_SIZE = 100000` (rows per chunk)
- **Data quality thresholds**:
  - `MIN_QUANTITY = 0` (set to 1 to exclude zero-quantity records)
  - `MIN_PRICE`, `MAX_PRICE`, `MIN_DISCOUNT`, `MAX_DISCOUNT`
- **Region/Category mappings**: Add custom normalization rules
- **Date formats**: Add supported date formats
- **Output paths**: Customize output file locations

## 🧪 Testing

```bash
# Generate test data first
python3 -m src.utils.data_generator

# Run tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html
```

## 📊 Performance

- **Memory**: 500MB-2GB (processes 100K rows at a time)
- **Speed**: ~5-15 minutes for 100M rows (varies by hardware)
- **Scalability**: Can handle datasets larger than available RAM

## 🐛 Troubleshooting

**Out of Memory?** Reduce `CHUNK_SIZE` in `src/config/settings.py`

**File Not Found?** Generate sample data: `python3 -m src.utils.data_generator`

**Test Failures?** Generate data first: `python3 -m src.utils.data_generator`

---
