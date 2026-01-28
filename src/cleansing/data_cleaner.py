"""
Data cleansing and normalization module to handle data quality issues including duplicates, invalid values, and inconsistencies.
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple
import logging
import re

from src.config.settings import (
    REGION_MAPPING, CATEGORY_MAPPING, DATE_FORMATS,
    MIN_QUANTITY, MAX_QUANTITY, MIN_PRICE, MAX_PRICE,
    MIN_DISCOUNT, MAX_DISCOUNT
)

logger = logging.getLogger(__name__)


class DataQualityIssues:
    """Track data quality issues found during cleansing."""
    
    def __init__(self):
        self.issues = {
            'duplicate_orders': 0,
            'invalid_quantity': 0,
            'invalid_price': 0,
            'invalid_discount': 0,
            'invalid_date': 0,
            'missing_values': 0,
            'normalized_regions': 0,
            'normalized_categories': 0,
            'total_rows_processed': 0,
            'total_rows_cleaned': 0,
        }
    
    def add_issue(self, issue_type: str, count: int = 1):
        """Add count to specific issue type."""
        if issue_type in self.issues:
            self.issues[issue_type] += count
    
    def get_report(self) -> dict:
        """Get quality issues report."""
        return self.issues.copy()


class DataCleaner:
    """
    Clean and normalize e-commerce sales data.
    
    Cleaning Rules:
    1. Remove duplicate order_ids (keep first occurrence)
    2. Convert quantity to numeric, remove invalid values
    3. Validate price and discount ranges
    4. Parse and standardize dates
    5. Normalize region and category names
    6. Calculate/validate revenue field
    """
    
    def __init__(self):
        self.quality_issues = DataQualityIssues()
    
    def clean_chunk(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean a chunk of data.
        
        Args:
            df: Input DataFrame chunk
            
        Returns:
            Cleaned DataFrame
        """
        self.quality_issues.add_issue('total_rows_processed', len(df))

        df_clean = df.copy()
        df_clean = self._remove_duplicates(df_clean)
        df_clean = self._clean_quantity(df_clean)
        df_clean = self._clean_price(df_clean)
        df_clean = self._clean_discount(df_clean)
        df_clean = self._clean_dates(df_clean)
        df_clean = self._normalize_region(df_clean)
        df_clean = self._normalize_category(df_clean)
        df_clean = self._calculate_revenue(df_clean)
        df_clean = self._handle_missing_values(df_clean)
        
        self.quality_issues.add_issue('total_rows_cleaned', len(df_clean))
        
        return df_clean
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate order_ids, keeping first occurrence."""
        initial_count = len(df)
        df_dedup = df.drop_duplicates(subset=['order_id'], keep='first')
        duplicates_removed = initial_count - len(df_dedup)
        
        if duplicates_removed > 0:
            self.quality_issues.add_issue('duplicate_orders', duplicates_removed)
            logger.debug(f"Removed {duplicates_removed} duplicate orders")
        
        return df_dedup
    
    def _clean_quantity(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert quantity to numeric and validate range.

        Note: MIN_QUANTITY is configurable in settings.py
        - Set to 1 for sales data (default) - quantity must be at least 1
        - Set to 0 to allow zero-quantity records (e.g., cancellations, returns)
        """
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
        invalid_mask = (
            df['quantity'].isna() |
            (df['quantity'] < MIN_QUANTITY) |
            (df['quantity'] > MAX_QUANTITY)
        )
        invalid_count = invalid_mask.sum()

        if invalid_count > 0:
            self.quality_issues.add_issue('invalid_quantity', invalid_count)
            logger.debug(f"Found {invalid_count} invalid quantity values (< {MIN_QUANTITY} or > {MAX_QUANTITY})")

        df.loc[invalid_mask, 'quantity'] = np.nan

        return df

    def _clean_price(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate unit_price range."""
        invalid_mask = (
            df['unit_price'].isna() |
            (df['unit_price'] < MIN_PRICE) |
            (df['unit_price'] > MAX_PRICE)
        )
        invalid_count = invalid_mask.sum()

        if invalid_count > 0:
            self.quality_issues.add_issue('invalid_price', invalid_count)
            logger.debug(f"Found {invalid_count} invalid price values")

        df.loc[invalid_mask, 'unit_price'] = np.nan

        return df

    def _clean_discount(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate discount_percent range."""
        invalid_mask = (
            df['discount_percent'].isna() |
            (df['discount_percent'] < MIN_DISCOUNT) |
            (df['discount_percent'] > MAX_DISCOUNT)
        )
        invalid_count = invalid_mask.sum()

        if invalid_count > 0:
            self.quality_issues.add_issue('invalid_discount', invalid_count)
            logger.debug(f"Found {invalid_count} invalid discount values")
        df.loc[invalid_mask, 'discount_percent'] = 0

        return df

    def _clean_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse dates from multiple formats."""
        df['sale_date_parsed'] = pd.NaT

        for date_format in DATE_FORMATS:
            mask = df['sale_date_parsed'].isna() & df['sale_date'].notna()
            if mask.sum() == 0:
                break

            try:
                df.loc[mask, 'sale_date_parsed'] = pd.to_datetime(
                    df.loc[mask, 'sale_date'],
                    format=date_format,
                    errors='coerce'
                )
            except Exception:
                continue

        # Count invalid dates
        invalid_count = df['sale_date_parsed'].isna().sum()
        if invalid_count > 0:
            self.quality_issues.add_issue('invalid_date', invalid_count)
            logger.debug(f"Found {invalid_count} invalid date values")

        df['sale_date'] = df['sale_date_parsed']
        df.drop('sale_date_parsed', axis=1, inplace=True)

        return df

    def _normalize_region(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize region names using mapping."""
        df['region_lower'] = df['region'].str.lower().str.strip()
        df['region_normalized'] = df['region_lower'].map(REGION_MAPPING)


        normalized_count = (
            df['region_normalized'].notna() &
            (df['region'] != df['region_normalized'])
        ).sum()

        if normalized_count > 0:
            self.quality_issues.add_issue('normalized_regions', normalized_count)
            logger.debug(f"Normalized {normalized_count} region values")

        df['region'] = df['region_normalized'].fillna(df['region'])
        df.drop(['region_lower', 'region_normalized'], axis=1, inplace=True)

        return df

    def _normalize_category(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize category names using mapping."""
        df['category_lower'] = df['category'].str.lower().str.strip()
        df['category_normalized'] = df['category_lower'].map(CATEGORY_MAPPING)

        normalized_count = (
            df['category_normalized'].notna() &
            (df['category'] != df['category_normalized'])
        ).sum()

        if normalized_count > 0:
            self.quality_issues.add_issue('normalized_categories', normalized_count)
            logger.debug(f"Normalized {normalized_count} category values")


        df['category'] = df['category_normalized'].fillna(df['category'])
        df.drop(['category_lower', 'category_normalized'], axis=1, inplace=True)

        return df

    def _calculate_revenue(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate revenue from quantity, price, and discount."""
        df['revenue'] = (
            df['quantity'] *
            df['unit_price'] *
            (1 - df['discount_percent'] / 100)
        )

        df['revenue'] = df['revenue'].round(2)

        return df

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove rows with critical missing values."""
        critical_fields = ['order_id', 'quantity', 'unit_price', 'sale_date']

        initial_count = len(df)
        df_clean = df.dropna(subset=critical_fields)
        missing_count = initial_count - len(df_clean)

        if missing_count > 0:
            self.quality_issues.add_issue('missing_values', missing_count)
            logger.debug(f"Removed {missing_count} rows with missing critical values")

        return df_clean

    def get_quality_report(self) -> dict:
        """Get data quality report."""
        return self.quality_issues.get_report()

