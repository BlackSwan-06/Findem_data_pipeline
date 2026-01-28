# E-Commerce Data Pipeline

A scalable, single‑machine data pipeline that ingests, cleans, and aggregates large e‑commerce sales datasets (100M+ rows) into analytics‑ready reports with explicit data‑quality tracking.

## 📑 Table of Contents
- [Quick Start](#-quick-start)
- [Requirements Met](#-requirements-met)
- [Data Quality Issues](#-data-quality-issues-handled)
- [Project Structure](#-project-structure)
- [Installation](#-installation)
- [Usage](#-usage)
- [Configuration](#️-configuration)
- [Output Files](#-output-files)
- [Performance](#-performance-characteristics)
- [Testing](#-testing)
- [Extending](#-extending-the-pipeline)
- [Troubleshooting](#-troubleshooting)

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

## 🎯 Overview

This pipeline processes messy e-commerce sales data through a multi-stage ETL process:

1. **Ingestion**: Memory-efficient chunked reading of large CSV files
2. **Cleansing**: Comprehensive data quality checks and normalization
3. **Transformation**: Aggregation into three analytical datasets
4. **Output**: Generation of business intelligence reports + data quality metrics

## 🏗️ Architecture

```
┌─────────────────┐
│   Input CSV     │
│  (100M+ rows)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Ingestion     │  ← Chunked reading (100K rows/chunk)
│  (csv_reader)   │    Memory-efficient processing
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Cleansing     │  ← Data quality checks
│ (data_cleaner)  │    Normalization & validation
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Transformation  │  ← Aggregations
│  (aggregator)   │    Analytics generation
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│     Output      │  ← CSV reports
│   (3 files +    │    Quality metrics
│  quality report)│
└─────────────────┘
```

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

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- 4GB+ RAM recommended
- Sufficient disk space for data files

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd Findem
```

2. Create a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

### Generate Sample Data

Generate test data with realistic quality issues:

```bash
python3 -m src.utils.data_generator
```

This creates `data/ecommerce_sales.csv` with:
- **Dynamic date range**: Last 3 years up to today (always current)
- **Realistic quality issues**: Duplicates, typos, invalid values, mixed formats
- **Configurable size**: Default 1M rows (edit `data_generator.py` to change)

### Run the Pipeline

```bash
python3 -m src.pipeline
```

Or using the installed command:

```bash
pip install -e .
ecommerce-pipeline
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

Run the test suite:

```bash
# Generate test data first (required for tests)
python3 -m src.utils.data_generator

# Run all tests
pytest

# Run with coverage report
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/test_pipeline.py -v

# Run with verbose output
pytest -v
```

**Important**: Tests use the actual generated CSV file (`data/ecommerce_sales.csv`) instead of temporary files, ensuring tests validate real-world behavior.

Test coverage includes:
- ✅ Integration tests for complete pipeline
- ✅ Edge case handling (uneven chunking, partial chunks)
- ✅ Data quality validation
- ✅ Real data testing (no mocked data)

## 🔧 Data Cleaning Rules

### Duplicate Removal
- **Rule**: Keep first occurrence of duplicate `order_id`
- **Impact**: Prevents double-counting of sales

### Quantity Validation
- **Rule**: Convert to numeric, remove if < MIN_QUANTITY (default: 0) or > 10,000
- **Default behavior**: Allows zero-quantity records (cancellations, returns, etc.)
- **Configurable**: Set `MIN_QUANTITY = 1` in settings.py to only allow actual sales
- **Handling**: Set invalid values (negative, non-numeric) to NaN, remove rows with missing quantity

### Price Validation
- **Rule**: Must be between $0.01 and $100,000
- **Handling**: Set invalid values to NaN, remove rows with missing price

### Discount Validation
- **Rule**: Must be between 0% and 100%
- **Handling**: Set invalid values to 0% (no discount)

### Date Parsing
- **Supported formats**:
  - `YYYY-MM-DD` (ISO format)
  - `MM/DD/YYYY` (US format)
  - `DD/MM/YYYY` (European format)
  - `YYYY/MM/DD`
  - And more...
- **Handling**: Try all formats, set unparseable dates to NaN

### Region Normalization
- **Examples**:
  - `"north america"`, `"n. america"`, `"NA"` → `"North America"`
  - `"europe"`, `"EU"`, `"europa"` → `"Europe"`
  - `"asia"`, `"asian"` → `"Asia"`

### Category Normalization
- **Examples**:
  - `"electronics"`, `"electronic"`, `"electrnics"` → `"Electronics"`
  - `"clothing"`, `"clothes"`, `"apparel"` → `"Clothing"`
  - `"home and garden"`, `"home"` → `"Home & Garden"`

## 📊 Performance Characteristics

### Memory Efficiency
- **Chunked processing**: Processes 100K rows at a time
- **Memory footprint**: Typically 500MB-2GB depending on data characteristics and chunk size
- **Scalability**: Can handle datasets larger than available RAM

### Processing Speed
- **Throughput**: Varies significantly based on hardware, disk I/O, and data complexity
- **Ballpark estimate**: 100M rows may take 5-15 minutes on modern hardware with SSD
- **Bottlenecks**: I/O (disk read/write) is typically the primary bottleneck
- **Note**: Performance depends on CPU, RAM, disk speed, and data characteristics

### Optimization Techniques
1. **Chunked reading**: Prevents loading entire dataset into memory
2. **Efficient data types**: Uses appropriate dtypes to reduce memory
3. **Vectorized operations**: Leverages pandas/numpy for speed
4. **Incremental aggregation**: Aggregates data as it's processed

## 🎯 Key Design Decisions

### Dynamic Date Generation
- **Date range**: Automatically generates data for the last 3 years up to today
- **Benefit**: Data always appears current without manual updates
- **Implementation**: `datetime.now()` - `timedelta(days=3*365)`

### Configurable Validation Thresholds
- **MIN_QUANTITY = 0**: Allows zero-quantity records by default (cancellations, returns)
- **Customizable**: Set to 1 in `settings.py` to only allow actual sales
- **Rationale**: Real e-commerce data includes non-sale transactions

### Real Data Testing
- **Tests use actual CSV**: No temporary files or mocked data
- **Benefit**: Tests validate real-world behavior
- **Graceful handling**: Tests skip if data file doesn't exist (with helpful message)

### Focused Test Suite
- **Unit tests**: Test individual cleaning methods
- **Integration tests**: Test complete pipeline workflow
- **No redundant tests**: Removed tests for basic Python/pandas functionality
- **Focus**: Business logic, not framework features

### JSON Serialization
- **Automatic type conversion**: Converts numpy int64 → Python int for JSON compatibility
- **Transparent**: No manual intervention needed
- **Robust**: Handles nested dictionaries and lists

## 🛠️ Advanced Usage

### Custom Chunk Size

```python
from src.pipeline import EcommercePipeline

# Use smaller chunks for limited memory
pipeline = EcommercePipeline(chunk_size=50000)
pipeline.run()
```

### Processing Specific Columns

```python
from src.ingestion.csv_reader import ChunkedCSVReader

reader = ChunkedCSVReader("data/ecommerce_sales.csv")
columns = ['order_id', 'revenue', 'sale_date']

for chunk in reader.read_chunks(columns=columns):
    # Process only selected columns
    pass
```

### Custom Cleaning Rules

Extend `DataCleaner` class:

```python
from src.cleansing.data_cleaner import DataCleaner

class CustomCleaner(DataCleaner):
    def clean_chunk(self, df):
        df = super().clean_chunk(df)
        # Add custom cleaning logic
        return df
```

## 📝 Logging

Logs are written to:
- **Console**: Real-time progress updates
- **File**: `logs/pipeline.log` (detailed execution log)

Log levels:
- `INFO`: Pipeline progress and summaries
- `DEBUG`: Detailed cleaning operations
- `ERROR`: Failures and exceptions

Configure log level via environment variable:
```bash
export LOG_LEVEL=DEBUG
python3 -m src.pipeline
```

## 🐛 Troubleshooting

### Out of Memory Error
- **Solution**: Reduce `CHUNK_SIZE` in `src/config/settings.py`
- **Example**: Change from 100000 to 50000

### File Not Found
- **Solution**: Ensure input file exists at `data/ecommerce_sales.csv`
- **Generate sample data**: `python3 -m src.utils.data_generator`

### JSON Serialization Error (int64)
- **Fixed**: Pipeline automatically converts numpy int64 types to native Python int
- **No action needed**: This is handled automatically in the quality report generation

### Slow Performance
- **Check**: Disk I/O speed (use SSD if possible)
- **Optimize**: Increase chunk size if you have more RAM
- **Monitor**: Check `logs/pipeline.log` for bottlenecks

### Test Failures
- **Generate data first**: Tests require `data/ecommerce_sales.csv` to exist
- **Command**: `python3 -m src.utils.data_generator` before running tests
- **Dependencies**: Ensure all dependencies are installed (`pip install -r requirements.txt`)
- **Python version**: Requires Python 3.8+

## 📚 Dependencies

Core libraries:
- **pandas** (≥2.0.0): Data manipulation and analysis
- **numpy** (≥1.24.0): Numerical operations

Development tools:
- **pytest** (≥7.4.0): Testing framework
- **pytest-cov** (≥4.1.0): Code coverage
- **black** (≥23.0.0): Code formatting
- **flake8** (≥6.0.0): Linting
- **mypy** (≥1.4.0): Type checking

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests (`pytest`)
5. Format code (`black src/ tests/`)
6. Commit changes (`git commit -m 'Add amazing feature'`)
7. Push to branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

---