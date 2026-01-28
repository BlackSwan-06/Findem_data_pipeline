"""
Configuration settings for the data pipeline.
"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
OUTPUT_DIR = DATA_DIR / "output"

# Create directories if they don't exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Data ingestion settings
CHUNK_SIZE = 100000  # Process 100K rows at a time for memory efficiency
INPUT_FILE = DATA_DIR / "ecommerce_sales.csv"

# Data quality thresholds
MIN_QUANTITY = 0  # Allow zero-quantity records (e.g., cancellations, returns).Set to 1 if you only want actual sales (quantity > 0)Note: Zero quantity records will have $0 revenue
MAX_QUANTITY = 10000
MIN_PRICE = 0.01
MAX_PRICE = 100000
MIN_DISCOUNT = 0
MAX_DISCOUNT = 100


REGION_MAPPING = {
    "north america": "North America",
    "n. america": "North America",
    "n america": "North America",
    "northamerica": "North America",
    "na": "North America",

    "europe": "Europe",
    "eu": "Europe",
    "europa": "Europe",

    "asia": "Asia",
    "asian": "Asia",

    "south america": "South America",
    "s. america": "South America",
    "s america": "South America",
    "southamerica": "South America",
    "sa": "South America",

    "africa": "Africa",
    "african": "Africa",

    "oceania": "Oceania",
    "australia": "Oceania",
    "pacific": "Oceania",
}

CATEGORY_MAPPING = {
    "electronics": "Electronics",
    "electronic": "Electronics",
    "electrnics": "Electronics",
    "elctronics": "Electronics",
    
    "clothing": "Clothing",
    "clothes": "Clothing",
    "apparel": "Clothing",
    "fashion": "Clothing",
    
    "home & garden": "Home & Garden",
    "home and garden": "Home & Garden",
    "home": "Home & Garden",
    "garden": "Home & Garden",
    
    "sports": "Sports",
    "sport": "Sports",
    "sporting goods": "Sports",
    
    "books": "Books",
    "book": "Books",
    
    "toys": "Toys",
    "toy": "Toys",
    "toys & games": "Toys",
    
    "food": "Food & Beverage",
    "beverage": "Food & Beverage",
    "food & beverage": "Food & Beverage",
}

DATE_FORMATS = [
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%d/%m/%Y",
    "%Y/%m/%d",
    "%m-%d-%Y",
    "%d-%m-%Y",
    "%Y%m%d",
    "%m/%d/%y",
    "%d/%m/%y",
]

MONTHLY_SALES_FILE = OUTPUT_DIR / "monthly_sales_summary.csv"
TOP_PRODUCTS_FILE = OUTPUT_DIR / "top_products.csv"
ANOMALY_RECORDS_FILE = OUTPUT_DIR / "anomaly_records.csv"
DATA_QUALITY_REPORT_FILE = OUTPUT_DIR / "data_quality_report.json"

LOG_FILE = LOGS_DIR / "pipeline.log"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

MAX_WORKERS = os.cpu_count() or 4  # For parallel processing (future enhancement)
MEMORY_LIMIT_GB = 4  # Target memory usage limit



