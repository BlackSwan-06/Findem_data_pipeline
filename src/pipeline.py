"""
Main data pipeline orchestrator to coordinate ingestion, cleansing, transformation, and report generation.
"""
import logging
import json
import time
from pathlib import Path
from typing import Optional

from src.config.settings import (
    INPUT_FILE, CHUNK_SIZE, LOG_FILE, LOG_LEVEL, LOG_FORMAT,
    MONTHLY_SALES_FILE, TOP_PRODUCTS_FILE, ANOMALY_RECORDS_FILE,
    DATA_QUALITY_REPORT_FILE)
from src.ingestion.csv_reader import ChunkedCSVReader
from src.cleansing.data_cleaner import DataCleaner
from src.transformation.aggregator import DataAggregator

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataPipeline:
    """
    Data pipeline for sales data with the following stages:
    1. Ingestion: Read large CSV in chunks
    2. Cleansing: Clean and normalize data
    3. Transformation: Aggregate into analytical datasets
    4. Reports: Save results to CSV files
    """
    
    def __init__(self, input_file: Optional[Path] = None, chunk_size: Optional[int] = None):
        """
        Initialize the pipeline.
        
        Args:
            input_file: Path to input CSV file (default: from config)
            chunk_size: Chunk size for processing (default: from config)
        """
        self.input_file = input_file or INPUT_FILE
        self.chunk_size = chunk_size or CHUNK_SIZE
        self.reader = ChunkedCSVReader(self.input_file, self.chunk_size)
        self.cleaner = DataCleaner()
        self.aggregator = DataAggregator()
        self.start_time = None
        self.end_time = None
        
    def run(self):
        """Execute the complete pipeline."""
        logger.info("=" * 80)
        logger.info("Starting Data Pipeline")
        logger.info("=" * 80)
        self.start_time = time.time()
        try:
            self._log_file_info()
            self._process_chunks()
            self._generate_outputs()
            self._generate_quality_report()
            self.end_time = time.time()
            self._log_summary()
            logger.info("=" * 80)
            logger.info("Pipeline completed successfully!")
            logger.info("=" * 80)
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise
    
    def _log_file_info(self):
        logger.info("-" * 80)
        logger.info("Stage 1: File Information")
        logger.info("-" * 80)
        file_info = self.reader.get_file_info()
        logger.info(f"Input file: {file_info['file_path']}")
        logger.info(f"File size: {file_info['file_size_mb']:.2f} MB ({file_info['file_size_gb']:.2f} GB)")
        logger.info(f"Columns: {', '.join(file_info['columns'])}")
        logger.info(f"Chunk size: {self.chunk_size:,} rows")
    
    def _process_chunks(self):
        """Process data chunks through cleaning and aggregation."""
        logger.info("-" * 80)
        logger.info("Stage 2: Processing Data Chunks")
        logger.info("-" * 80)
        
        chunk_count = 0
        
        for chunk in self.reader.read_chunks():
            chunk_count += 1
            cleaned_chunk = self.cleaner.clean_chunk(chunk)
            if len(cleaned_chunk) > 0:
                self.aggregator.process_chunk(cleaned_chunk)
            logger.info(
                f"Chunk {chunk_count}: "
                f"Input={len(chunk):,} rows, "
                f"Cleaned={len(cleaned_chunk):,} rows"
            )
        logger.info(f"Total chunks processed: {chunk_count}")
    
    def _generate_outputs(self):
        """Generate and save analytical outputs."""
        logger.info("-" * 80)
        logger.info("Stage 3: Generating Analytical Outputs")
        logger.info("-" * 80)
        
        # 1. Monthly sales summary
        monthly_sales = self.aggregator.get_monthly_sales_summary()
        monthly_sales.to_csv(MONTHLY_SALES_FILE, index=False)
        logger.info(f"✓ Monthly sales summary saved: {MONTHLY_SALES_FILE}")
        logger.info(f"  - {len(monthly_sales)} months")
        
        # 2. Top products
        top_products = self.aggregator.get_top_products(top_n=10)
        top_products.to_csv(TOP_PRODUCTS_FILE, index=False)
        logger.info(f"✓ Top products saved: {TOP_PRODUCTS_FILE}")
        logger.info(f"  - {len(top_products)} products")
        
        # 3. Anomaly records
        anomalies = self.aggregator.get_anomaly_records(top_n=5)
        anomalies.to_csv(ANOMALY_RECORDS_FILE, index=False)
        logger.info(f"✓ Anomaly records saved: {ANOMALY_RECORDS_FILE}")
        logger.info(f"  - {len(anomalies)} records")

    def _generate_quality_report(self):
        """Generate and save data quality report."""
        logger.info("-" * 80)
        logger.info("Stage 4: Data Quality Report")
        logger.info("-" * 80)
        quality_report = self.cleaner.get_quality_report()
        total_processed = quality_report['total_rows_processed']
        total_cleaned = quality_report['total_rows_cleaned']
        rows_removed = total_processed - total_cleaned
        quality_report['rows_removed'] = rows_removed
        quality_report['data_quality_score'] = round(
            (total_cleaned / total_processed * 100) if total_processed > 0 else 0,
            2
        )

        # Convert numpy/pandas types to native Python types for JSON serialization
        def convert_to_native(obj):
            if hasattr(obj, 'item'):  # numpy types
                return obj.item()
            elif isinstance(obj, dict):
                return {k: convert_to_native(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_to_native(item) for item in obj]
            return obj

        quality_report = convert_to_native(quality_report)
        with open(DATA_QUALITY_REPORT_FILE, 'w') as f:
            json.dump(quality_report, f, indent=2)

        logger.info(f"✓ Data quality report saved: {DATA_QUALITY_REPORT_FILE}")
        logger.info(f"  - Total rows processed: {total_processed:,}")
        logger.info(f"  - Total rows cleaned: {total_cleaned:,}")
        logger.info(f"  - Rows removed: {rows_removed:,}")
        logger.info(f"  - Data quality score: {quality_report['data_quality_score']}%")
        logger.info("  Quality Issues Found:")
        for issue, count in quality_report.items():
            if issue not in ['total_rows_processed', 'total_rows_cleaned', 'rows_removed', 'data_quality_score']:
                if count > 0:
                    logger.info(f"    - {issue}: {count:,}")

    def _log_summary(self):
        """Log pipeline execution summary."""
        logger.info("-" * 80)
        logger.info("Pipeline Summary")
        logger.info("-" * 80)
        duration = self.end_time - self.start_time
        minutes = int(duration // 60)
        seconds = int(duration % 60)
        logger.info(f"Execution time: {minutes}m {seconds}s")
        logger.info(f"Total rows processed: {self.reader.total_rows:,}")
        logger.info(f"Chunks processed: {self.reader.chunks_processed}")
        logger.info("\nOutput files:")
        logger.info(f"  - {MONTHLY_SALES_FILE}")
        logger.info(f"  - {TOP_PRODUCTS_FILE}")
        logger.info(f"  - {ANOMALY_RECORDS_FILE}")
        logger.info(f"  - {DATA_QUALITY_REPORT_FILE}")


def main():
    pipeline = DataPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()

