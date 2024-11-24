"""
src/batch_processor.py
Main batch processing system that handles record batching with size constraints.
"""

import logging
from typing import List, Iterator, Dict
import sys

from .batch_constraints import BatchConstraints
from .batch_metrics import BatchMetrics

# Configure logging with both file and console output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('batch_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BatchProcessor:
    """Processes records into batches according to size and count constraints.

        Attributes:
            constraints: BatchConstraints instance defining processing limits
            metrics: BatchMetrics instance tracking processing statistics
    """
    
    def __init__(self, constraints: BatchConstraints = BatchConstraints()) -> None:
        """Initialize processor with given constraints.
        
            Args:
                constraints: Optional BatchConstraints instance, uses defaults if not provided
        """

        logger.info("Initializing BatchProcessor with constraints: max_record=%d bytes, max_batch=%d bytes, max_records=%d",
                   constraints.max_record_size_bytes,
                   constraints.max_batch_size_bytes,
                   constraints.max_records_per_batch)
        self.constraints = constraints
        self.metrics = BatchMetrics()

    def _get_record_size(self, record: str) -> int:
        """Calculate size of record in bytes.

            Args:
                record: String record to measure

            Returns:
                Size of record in bytes
        """

        return sys.getsizeof(record.encode('utf-8'))

    def is_valid_record(self, record: str) -> bool:
        """Check if record meets size constraints.
            
            Args:
                record: String record to validate

            Returns:
                True if record size is within constraints, False otherwise

            Raises:
                TypeError: If record is not a string
        """
        
        if not isinstance(record, str):
            raise TypeError(f"Record must be string, got {type(record).__name__}")
        
        try:
            record_size = self._get_record_size(record)
            is_valid = record_size <= self.constraints.max_record_size_bytes
            
            if not is_valid:
                logger.warning(
                    f"Record discarded: size {record_size} bytes exceeds limit"
                )
                self.metrics.records_discarded += 1
            return is_valid
        except Exception as e:
            raise TypeError(f"Error processing record: {str(e)}")

    def create_batches(self, records: List[str]) -> Iterator[List[str]]:
        """Process records into appropriately sized batches.
            
            Args:
                records: List of string records to process

            Returns:
                Iterator yielding lists of records as batches

            Raises:
                TypeError: If records is not a list of strings
        """

        if records is None or not isinstance(records, list):
            raise TypeError("'records' must be a non-empty list")
        
        try:
            logger.info("Starting batch processing for %d records", len(records))
            current_batch: List[str] = []
            current_batch_size: int = 0

            for idx, record in enumerate(records, 1):
                logger.debug("Processing record %d/%d", idx, len(records))
                self.metrics.records_processed += 1

                if not self.is_valid_record(record):
                    logger.info("Record %d skipped: size validation failed", idx)
                    continue

                record_size = self._get_record_size(record)

                if current_batch_size + record_size > self.constraints.max_batch_size_bytes or len(current_batch) >= self.constraints.max_records_per_batch:
                    if current_batch:
                        logger.info("Yielding batch: %d records, %d bytes", len(current_batch), current_batch_size)
                        self.metrics.batches_created += 1
                        yield current_batch
                    current_batch = []
                    current_batch_size = 0

                current_batch.append(record)
                current_batch_size += record_size
                self.metrics.total_bytes_processed += record_size
                logger.debug("Added record to batch. Current batch: %d records, %d bytes", len(current_batch), current_batch_size)

            if current_batch:
                logger.info("Yielding final batch: %d records, %d bytes", len(current_batch), current_batch_size)
                self.metrics.batches_created += 1
                yield current_batch

            logger.info("Batch processing completed. Metrics: %s", self.get_metrics())

        except Exception as e:
            logger.error("Error processing batch: %s", str(e), exc_info=True)
            raise

    def get_metrics(self) -> Dict[str, int]:
        """Return current processing metrics.
        
            Returns:
                Dictionary containing current metric values
        """

        metrics = self.metrics.to_dict()
        logger.debug("Current metrics: %s", metrics)
        return metrics


def process_records(records: List[str], 
                   constraints: BatchConstraints = BatchConstraints()) -> List[List[str]]:
    """Convenience function for batch processing.

        Args:
            records: List of string records to process
            constraints: Optional BatchConstraints instance

        Returns:
            List of batches, where each batch is a list of records
    """
    
    logger.info("Processing records using convenience function")
    processor = BatchProcessor(constraints)
    return list(processor.create_batches(records))
