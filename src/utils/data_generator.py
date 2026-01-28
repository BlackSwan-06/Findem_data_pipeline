import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class EcommerceDataGenerator:
    """Generate e-commerce sales data with quality issues."""
    
    def __init__(self, num_rows: int = 1000000):
        self.num_rows = num_rows
        
        # Product catalog
        self.products = [
            ("Laptop Pro 15", "Electronics"),
            ("Wireless Mouse", "Electronics"),
            ("USB-C Cable", "Electronics"),
            ("Running Shoes", "Clothing"),
            ("Cotton T-Shirt", "Clothing"),
            ("Garden Hose", "Home & Garden"),
            ("LED Light Bulb", "Home & Garden"),
            ("Basketball", "Sports"),
            ("Yoga Mat", "Sports"),
            ("Mystery Novel", "Books"),
            ("Action Figure", "Toys"),
            ("Coffee Beans", "Food & Beverage"),
        ]
        
        # Regions with typos/variants
        self.regions = [
            "North America", "n. america", "N America", "northamerica",
            "Europe", "EU", "europa",
            "Asia", "asian",
            "South America", "s. america", "SA",
            "Africa", "african",
            "Oceania", "australia",
        ]
        
        # Date range - generate data for the last 3 years up to today
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=3*365)  # 3 years of data
        
    def generate_row(self, row_num: int) -> dict:
        """Generate a single row"""

        product_name, category = random.choice(self.products)
        quantity = random.randint(1, 10000)
        unit_price = round(random.uniform(5, 500), 2)
        discount_percent = round(random.uniform(0, 30), 1)
        region = random.choice(self.regions)

        days_diff = (self.end_date - self.start_date).days
        sale_date = self.start_date + timedelta(days=random.randint(0, days_diff))
        order_id = f"ORD{row_num:08d}"
        
        # Introduce quality issues (10% of records)
        if random.random() < 0.1:
            issue_type = random.choice([
                "duplicate_order",
                "dirty_category",
                "string_quantity",
                "negative_quantity",
                "invalid_price",
                "invalid_discount",
                "null_date",
                "wrong_date_format",
                "typo_region",
            ])
            
            if issue_type == "duplicate_order":
                order_id = f"ORD{row_num - random.randint(1, 100):08d}"
            elif issue_type == "dirty_category":
                category = category.lower() if random.random() < 0.5 else category.replace("&", "and")
            elif issue_type == "string_quantity":
                quantity = str(quantity)
            elif issue_type == "negative_quantity":
                quantity = -random.randint(1, 10)
            elif issue_type == "invalid_price":
                unit_price = random.choice([0, -10.5, 999999])
            elif issue_type == "invalid_discount":
                discount_percent = random.choice([-5, 150, 999])
            elif issue_type == "null_date":
                sale_date = None
            elif issue_type == "wrong_date_format":
                pass
            elif issue_type == "typo_region":
                region = region.lower()

        if sale_date:
            date_format = random.choice([
                "%Y-%m-%d",
                "%m/%d/%Y",
                "%d/%m/%Y",
                "%Y/%m/%d",
            ])
            sale_date_str = sale_date.strftime(date_format)
        else:
            sale_date_str = ""

        revenue = quantity * unit_price * (1 - discount_percent / 100) if isinstance(quantity, int) else 0
        
        return {
            "order_id": order_id,
            "product_name": product_name,
            "category": category,
            "quantity": quantity,
            "unit_price": unit_price,
            "discount_percent": discount_percent,
            "region": region,
            "sale_date": sale_date_str,
            "revenue": round(revenue, 2) if revenue else "",
        }
    
    def generate_csv(self, output_path: Path, chunk_size: int = 100000):
        """Generate CSV file in chunks to manage memory."""
        logger.info(f"Generating {self.num_rows:,} rows of e-commerce data...")
        
        fieldnames = [
            "order_id", "product_name", "category", "quantity",
            "unit_price", "discount_percent", "region", "sale_date", "revenue"
        ]
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for i in range(1, self.num_rows + 1):
                row = self.generate_row(i)
                writer.writerow(row)
                
                if i % chunk_size == 0:
                    logger.info(f"Generated {i:,} rows...")
        
        logger.info(f"Data generation complete: {output_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from src.config.settings import DATA_DIR
    
    # Generate sample data
    generator = EcommerceDataGenerator(num_rows=1000000)  # 1M for testing
    generator.generate_csv(DATA_DIR / "ecommerce_sales.csv")

