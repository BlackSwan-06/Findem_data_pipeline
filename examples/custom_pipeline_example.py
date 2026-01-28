"""
Example: Custom pipeline with modified settings and custom cleaning rules.

This example demonstrates how to:
1. Customize pipeline settings
2. Extend the DataCleaner with custom rules
3. Add custom aggregations
4. Process data with custom logic
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from src.pipeline import EcommercePipeline
from src.cleansing.data_cleaner import DataCleaner
from src.transformation.aggregator import DataAggregator
from src.config.settings import DATA_DIR


class CustomDataCleaner(DataCleaner):
    """Extended data cleaner with custom rules."""
    
    def clean_chunk(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply standard cleaning plus custom rules."""
        # Apply standard cleaning
        df = super().clean_chunk(df)
        
        # Custom rule: Flag high-value orders
        df['is_high_value'] = df['revenue'] > 1000
        
        # Custom rule: Categorize discount levels
        df['discount_category'] = pd.cut(
            df['discount_percent'],
            bins=[0, 10, 20, 100],
            labels=['Low', 'Medium', 'High']
        )
        
        return df


class CustomDataAggregator(DataAggregator):
    """Extended aggregator with custom analytics."""
    
    def __init__(self):
        super().__init__()
        self.region_sales = {}
    
    def process_chunk(self, df: pd.DataFrame):
        """Process chunk with standard and custom aggregations."""
        # Standard aggregations
        super().process_chunk(df)
        
        # Custom aggregation: Sales by region
        region_chunk = df.groupby('region').agg({
            'revenue': 'sum',
            'quantity': 'sum',
        }).reset_index()
        
        for _, row in region_chunk.iterrows():
            region = row['region']
            self.region_sales[region] = self.region_sales.get(region, 0) + row['revenue']
    
    def get_region_sales(self) -> pd.DataFrame:
        """Get sales by region."""
        if not self.region_sales:
            return pd.DataFrame()
        
        df = pd.DataFrame({
            'region': list(self.region_sales.keys()),
            'total_revenue': list(self.region_sales.values()),
        })
        
        df = df.sort_values('total_revenue', ascending=False)
        df['total_revenue'] = df['total_revenue'].round(2)
        
        return df


class CustomPipeline(EcommercePipeline):
    """Custom pipeline with extended functionality."""
    
    def __init__(self, input_file=None, chunk_size=None):
        super().__init__(input_file, chunk_size)
        
        # Replace standard components with custom ones
        self.cleaner = CustomDataCleaner()
        self.aggregator = CustomDataAggregator()
    
    def _generate_outputs(self):
        """Generate standard outputs plus custom ones."""
        # Generate standard outputs
        super()._generate_outputs()
        
        # Generate custom output: Region sales
        region_sales = self.aggregator.get_region_sales()
        output_file = DATA_DIR / "output" / "region_sales.csv"
        region_sales.to_csv(output_file, index=False)
        print(f"✓ Region sales saved: {output_file}")
        print(f"  - {len(region_sales)} regions")


def main():
    """Run custom pipeline example."""
    print("=" * 80)
    print("Custom Pipeline Example")
    print("=" * 80)
    print()
    
    # Create and run custom pipeline with smaller chunk size
    pipeline = CustomPipeline(chunk_size=50000)
    pipeline.run()
    
    print()
    print("Custom outputs generated:")
    print("  - data/output/region_sales.csv")


if __name__ == "__main__":
    main()

