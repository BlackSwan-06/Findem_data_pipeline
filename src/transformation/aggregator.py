"""Data transformation and aggregation module that produces analytical datasets from cleaned data."""
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class DataAggregator:
    """
    Aggregate cleaned data into analytical datasets.
    
    Produces:
    1. Monthly sales summary (revenue, quantity, avg discount by month)
    2. Top products (top 10 by revenue and units)
    3. Anomaly records (top 5 highest-revenue records)
    """
    
    def __init__(self):
        self.monthly_sales = []
        self.product_revenue = {}
        self.product_units = {}
        self.all_records = []
    
    def process_chunk(self, df: pd.DataFrame):
        """
        Process a chunk of cleaned data for aggregation.
        
        Args:
            df: Cleaned DataFrame chunk
        """
        df['year_month'] = df['sale_date'].dt.to_period('M')
        monthly_chunk = df.groupby('year_month').agg({
            'revenue': 'sum',
            'quantity': 'sum',
            'discount_percent': 'mean',
        }).reset_index()
        monthly_chunk.columns = ['year_month', 'total_revenue', 'total_quantity', 'avg_discount']
        self.monthly_sales.append(monthly_chunk)
        product_chunk = df.groupby('product_name').agg({
            'revenue': 'sum',
            'quantity': 'sum',
        }).reset_index()
        
        for _, row in product_chunk.iterrows():
            product = row['product_name']
            self.product_revenue[product] = self.product_revenue.get(product, 0) + row['revenue']
            self.product_units[product] = self.product_units.get(product, 0) + row['quantity']
        top_chunk = df.nlargest(1000, 'revenue')[
            ['order_id', 'product_name', 'category', 'quantity', 
             'unit_price', 'discount_percent', 'region', 'sale_date', 'revenue']
        ]
        self.all_records.append(top_chunk)
        logger.debug(f"Processed chunk: {len(df)} rows for aggregation")
    
    def get_monthly_sales_summary(self) -> pd.DataFrame:
        """
        Get monthly sales summary.
        
        Returns:
            DataFrame with columns: year_month, total_revenue, total_quantity, avg_discount
        """
        if not self.monthly_sales:
            logger.warning("No monthly sales data available")
            return pd.DataFrame()

        combined = pd.concat(self.monthly_sales, ignore_index=True)
        final_monthly = combined.groupby('year_month').agg({
            'total_revenue': 'sum',
            'total_quantity': 'sum',
            'avg_discount': 'mean',
        }).reset_index()
        final_monthly['year_month'] = final_monthly['year_month'].astype(str)
        final_monthly['total_revenue'] = final_monthly['total_revenue'].round(2)
        final_monthly['total_quantity'] = final_monthly['total_quantity'].astype(int)
        final_monthly['avg_discount'] = final_monthly['avg_discount'].round(2)
        final_monthly = final_monthly.sort_values('year_month')
        logger.info(f"Generated monthly sales summary: {len(final_monthly)} months")
        return final_monthly
    
    def get_top_products(self, top_n: int = 10) -> pd.DataFrame:
        """
        Get top products by revenue and units sold.
        
        Args:
            top_n: Number of top products to return (default: 10)
            
        Returns:
            DataFrame with top products
        """
        if not self.product_revenue:
            logger.warning("No product data available")
            return pd.DataFrame()
        products_df = pd.DataFrame({
            'product_name': list(self.product_revenue.keys()),
            'total_revenue': list(self.product_revenue.values()),
            'total_units_sold': [self.product_units[p] for p in self.product_revenue.keys()],
        })
        top_by_revenue = products_df.nlargest(top_n, 'total_revenue').copy()
        top_by_revenue['rank_by'] = 'revenue'
        top_by_units = products_df.nlargest(top_n, 'total_units_sold').copy()
        top_by_units['rank_by'] = 'units'
        top_products = pd.concat([top_by_revenue, top_by_units], ignore_index=True)
        top_products = top_products.drop_duplicates(subset=['product_name'], keep='first')
        top_products['total_revenue'] = top_products['total_revenue'].round(2)
        top_products['total_units_sold'] = top_products['total_units_sold'].astype(int)
        top_products = top_products.sort_values('total_revenue', ascending=False)
        logger.info(f"Generated top products: {len(top_products)} products")
        return top_products
    
    def get_anomaly_records(self, top_n: int = 5) -> pd.DataFrame:
        """
        Get anomaly records (highest revenue transactions).
        
        Args:
            top_n: Number of anomaly records to return (default: 5)
            
        Returns:
            DataFrame with anomaly records
        """
        if not self.all_records:
            logger.warning("No records available for anomaly detection")
            return pd.DataFrame()
        combined = pd.concat(self.all_records, ignore_index=True)
        anomalies = combined.nlargest(top_n, 'revenue')
        anomalies['sale_date'] = anomalies['sale_date'].dt.strftime('%Y-%m-%d')
        logger.info(f"Generated anomaly records: {len(anomalies)} records")
        return anomalies

