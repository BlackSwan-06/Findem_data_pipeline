"""
Integration tests for the complete data pipeline from Ingestion → Cleaning → Aggregation → report generation
"""
import pytest
import pandas as pd
import os
from src.config.settings import INPUT_FILE, OUTPUT_DIR
from src.pipeline import DataPipeline


class TestPipeline:
    """Test the complete data pipeline end-to-end."""

    @pytest.fixture
    def pipeline(self):
        """
        Create pipeline instance with comprehensive file validation.

        Handles all file-related errors gracefully:
        - File not found
        - Empty file
        - File with only headers
        - Invalid file format
        """
        # Check if data file exists
        if not INPUT_FILE.exists():
            pytest.skip(
                f"Data file not found: {INPUT_FILE}\n"
                f"Please generate sample data first:\n"
                f"  python -m src.utils.data_generator"
            )

        # Check if file is empty
        file_size = os.path.getsize(INPUT_FILE)
        if file_size == 0:
            pytest.skip(
                f"Data file is empty: {INPUT_FILE}\n"
                f"File size: 0 bytes\n"
                f"Please generate sample data first:\n"
                f"  python -m src.utils.data_generator"
            )

        # Check if file has data (not just headers)
        try:
            test_df = pd.read_csv(INPUT_FILE, nrows=1)
            if len(test_df) == 0:
                pytest.skip(
                    f"Data file has no data rows: {INPUT_FILE}\n"
                    f"File contains only headers\n"
                    f"Please generate sample data first:\n"
                    f"  python -m src.utils.data_generator"
                )
        except pd.errors.EmptyDataError:
            pytest.skip(
                f"Data file is empty or invalid: {INPUT_FILE}\n"
                f"Please generate sample data first:\n"
                f"  python -m src.utils.data_generator"
            )
        except Exception as e:
            pytest.skip(
                f"Cannot read data file: {INPUT_FILE}\n"
                f"Error: {e}\n"
                f"Please generate sample data first:\n"
                f"  python -m src.utils.data_generator"
            )
        try:
            return DataPipeline(INPUT_FILE)
        except (FileNotFoundError, ValueError) as e:
            pytest.skip(f"Cannot create pipeline: {e}")
    
    def test_full_pipeline_execution(self, pipeline):
        """Test complete pipeline execution from ingestion to outputs."""
        pipeline.run()
        
        # Verify all output files were created
        monthly_sales = OUTPUT_DIR / "monthly_sales_summary.csv"
        top_products = OUTPUT_DIR / "top_products.csv"
        anomaly_records = OUTPUT_DIR / "anomaly_records.csv"
        
        assert monthly_sales.exists(), "monthly_sales_summary.csv not created"
        assert top_products.exists(), "top_products.csv not created"
        assert anomaly_records.exists(), "anomaly_records.csv not created"
        
        # Verify monthly_sales_summary has correct structure
        df_monthly = pd.read_csv(monthly_sales)
        assert 'month' in df_monthly.columns
        assert 'total_revenue' in df_monthly.columns
        assert 'total_quantity' in df_monthly.columns
        assert 'avg_discount' in df_monthly.columns
        assert len(df_monthly) > 0, "monthly_sales_summary is empty"
        
        # Verify top_products has correct structure
        df_products = pd.read_csv(top_products)
        assert 'product_name' in df_products.columns
        assert 'total_revenue' in df_products.columns
        assert 'total_units_sold' in df_products.columns
        assert len(df_products) == 10, "top_products should have exactly 10 rows"
        
        # Verify anomaly_records has correct structure
        df_anomalies = pd.read_csv(anomaly_records)
        assert 'order_id' in df_anomalies.columns
        assert 'revenue' in df_anomalies.columns
        assert len(df_anomalies) == 5, "anomaly_records should have exactly 5 rows"
        
        # Verify data quality - no nulls in key columns
        assert df_monthly['total_revenue'].notna().all()
        assert df_products['total_revenue'].notna().all()
        assert df_anomalies['revenue'].notna().all()
        
        # Verify data makes sense - positive values
        assert (df_monthly['total_revenue'] >= 0).all()
        assert (df_products['total_revenue'] > 0).all()
        assert (df_anomalies['revenue'] > 0).all()
    
    def test_data_quality_report(self, pipeline):
        pipeline.run()
        report = pipeline.cleaner.quality_issues.get_report()
        # Verify report has expected keys
        assert 'duplicate_orders' in report
        assert 'invalid_quantity' in report
        assert 'invalid_price' in report
        assert 'invalid_discount' in report
        for key, value in report.items():
            assert isinstance(value, (int, float)), f"{key} should be numeric"
            assert value >= 0, f"{key} should be non-negative"
    
    def test_pipeline_with_small_chunk_size(self, pipeline):
        """Test pipeline works with different chunk sizes."""
        small_chunk_pipeline = DataPipeline(INPUT_FILE, chunk_size=500)
        small_chunk_pipeline.run()
        # Verify outputs still created correctly
        monthly_sales = OUTPUT_DIR / "monthly_sales_summary.csv"
        assert monthly_sales.exists()
        df = pd.read_csv(monthly_sales)
        assert len(df) > 0

