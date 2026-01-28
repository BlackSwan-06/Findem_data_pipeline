"""
CSV data ingestion using chunked reading.
"""
import pandas as pd
from pathlib import Path
from typing import Iterator, Optional
import logging
import os

logger = logging.getLogger(__name__)

class ChunkedCSVReader:
    """
    Read large CSV files in chunks to manage memory efficiently.
    """
    
    def __init__(self, file_path: Path, chunk_size: int = 100000):
        """
        Initialize the chunked CSV reader.

        Args:
            file_path: Path to the CSV file
            chunk_size: Number of rows to read per chunk (default: 100K)

        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the file is empty or invalid
        """
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.total_rows = 0
        self.chunks_processed = 0
        self._validate_file()

    def _validate_file(self):
        """
        Validate that the file exists and is not empty.

        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the file is empty or has no data rows
        """
        import os
       # Check if file exists
        if not self.file_path.exists():
            raise FileNotFoundError(
                f"CSV file not found: {self.file_path}\n"
                f"Please ensure the file exists or generate sample data with:\n"
                f"  python -m src.utils.data_generator"
            )
        # Check if file is empty
        file_size = os.path.getsize(self.file_path)
        if file_size == 0:
            raise ValueError(
                f"CSV file is empty: {self.file_path}\n"
                f"File size: 0 bytes\n"
                f"Please generate data with: python -m src.utils.data_generator"
            )
        # Check if file has at least a header row
        try:
            with open(self.file_path, 'r') as f:
                first_line = f.readline().strip()
                if not first_line:
                    raise ValueError(
                        f"CSV file has no content: {self.file_path}\n"
                        f"File appears to be empty or contains only whitespace"
                    )
                # Check if there's at least one data row
                second_line = f.readline().strip()
                if not second_line:
                    raise ValueError(
                        f"CSV file has no data rows: {self.file_path}\n"
                        f"File contains only a header row with no data\n"
                        f"Please generate data with: python -m src.utils.data_generator"
                    )
        except (IOError, OSError) as e:
            raise ValueError(f"Cannot read CSV file: {self.file_path}\nError: {e}")
        logger.info(f"File validation passed: {self.file_path} ({file_size:,} bytes)")

    def read_chunks(self, columns: Optional[list] = None) -> Iterator[pd.DataFrame]:
        """
        Read CSV file in chunks.
        
        Args:
            columns: Optional list of columns to read. If None, reads all columns.
            
        Yields:
            DataFrame chunks
        """
        logger.info(f"Starting chunked read of {self.file_path}")
        logger.info(f"Chunk size: {self.chunk_size:,} rows")
        try:
            dtype_spec = {
                'order_id': 'str',
                'product_name': 'str',
                'category': 'str',
                'quantity': 'object',  # Mixed types possible
                'unit_price': 'float64',
                'discount_percent': 'float64',
                'region': 'str',
                'sale_date': 'str',
                'revenue': 'object',  # May have nulls or strings
            }
            chunk_iterator = pd.read_csv(
                self.file_path,
                chunksize=self.chunk_size,
                dtype=dtype_spec,
                usecols=columns,
                low_memory=True,
                na_values=['', 'NULL', 'null', 'NA', 'N/A'],
                keep_default_na=True,
            )
            chunk_count = 0
            for chunk in chunk_iterator:
                chunk_count += 1
                self.chunks_processed += 1
                self.total_rows += len(chunk)

                logger.info(
                    f"Processed chunk {self.chunks_processed}: "
                    f"{len(chunk):,} rows (Total: {self.total_rows:,})"
                )

                yield chunk
            # Check if we processed any chunks
            if chunk_count == 0:
                raise ValueError(
                    f"No data found in CSV file: {self.file_path}\n"
                    f"File may be empty or contain only headers"
                )
        except FileNotFoundError:
            logger.error(f"File not found: {self.file_path}")
            raise
        except pd.errors.EmptyDataError:
            logger.error(f"CSV file is empty or has no data: {self.file_path}")
            raise ValueError(
                f"CSV file contains no data: {self.file_path}\n"
                f"Please generate data with: python -m src.utils.data_generator"
            )
        except pd.errors.ParserError as e:
            logger.error(f"CSV parsing error: {e}")
            raise ValueError(
                f"Invalid CSV format in file: {self.file_path}\n"
                f"Error: {e}"
            )
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise
    
    def get_sample(self, n_rows: int = 1000) -> pd.DataFrame:
        """
        Read a sample of rows from the CSV file.
        
        Args:
            n_rows: Number of rows to sample
            
        Returns:
            DataFrame with sample data
        """
        logger.info(f"Reading sample of {n_rows} rows from {self.file_path}")
        try:
            sample_df = pd.read_csv(self.file_path, nrows=n_rows)
            logger.info(f"Sample loaded: {len(sample_df)} rows, {len(sample_df.columns)} columns")
            return sample_df
        except Exception as e:
            logger.error(f"Error reading sample: {e}")
            raise
    
    def get_file_info(self) -> dict:
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {self.file_path}")
        file_size_bytes = os.path.getsize(self.file_path)
        if file_size_bytes == 0:
            raise ValueError(f"CSV file is empty: {self.file_path}")
        file_size_mb = file_size_bytes / (1024 * 1024)
        file_size_gb = file_size_mb / 1024
        # Read first chunk to get column info
        try:
            first_chunk = pd.read_csv(self.file_path, nrows=1)
        except pd.errors.EmptyDataError:
            raise ValueError(f"CSV file contains no data: {self.file_path}")
        info = {
            'file_path': str(self.file_path),
            'file_size_bytes': file_size_bytes,
            'file_size_mb': round(file_size_mb, 2),
            'file_size_gb': round(file_size_gb, 2),
            'columns': list(first_chunk.columns),
            'num_columns': len(first_chunk.columns),
        }
        logger.info(f"File info: {info['file_size_mb']:.2f} MB, {info['num_columns']} columns")
        return info
    
    def estimate_total_rows(self) -> int:
        """
        Estimate total number of rows by reading the file.
        Note: This can be slow for very large files.
        
        Returns:
            Estimated number of rows
        """
        logger.info("Estimating total rows (this may take a moment)...")
        row_count = 0
        for chunk in pd.read_csv(self.file_path, chunksize=self.chunk_size, usecols=[0]):
            row_count += len(chunk)
        logger.info(f"Estimated total rows: {row_count:,}")
        return row_count

