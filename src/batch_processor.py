"""
src/batch_processor.py
Batch processing system with enhanced logging
"""

import logging
from dataclasses import dataclass
from typing import List, Iterator, Final, Dict
import sys

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

@dataclass(frozen=True)
class BatchConstraints:
    """Defines size and count constraints for batch processing."""
    max_record_size_bytes: Final[int] = 1_000_000  # 1 MB
    max_batch_size_bytes: Final[int] = 5_000_000   # 5 MB
    max_records_per_batch: Final[int] = 500

@dataclass
class BatchMetrics:
    """Collects and maintains processing metrics."""
    records_processed: int = 0
    records_discarded: int = 0
    batches_created: int = 0
    total_bytes_processed: int = 0
    
    def to_dict(self) -> Dict[str, int]:
        """Convert metrics to dictionary format."""
        return {
            "records_processed": self.records_processed,
            "records_discarded": self.records_discarded,
            "batches_created": self.batches_created,
            "total_bytes_processed": self.total_bytes_processed
        }

class BatchProcessor:
    """Processes records into batches according to size and count constraints."""
    
    def __init__(self, constraints: BatchConstraints = BatchConstraints()) -> None:
        """Initialize processor with given constraints."""
        logger.info("Initializing BatchProcessor with constraints: max_record=%d bytes, max_batch=%d bytes, max_records=%d",
                   constraints.max_record_size_bytes,
                   constraints.max_batch_size_bytes,
                   constraints.max_records_per_batch)
        self.constraints = constraints
        self.metrics = BatchMetrics()


    def _get_record_size(self, record: str) -> int:
        """Calculate size of record in bytes."""
        return sys.getsizeof(record.encode('utf-8'))


    def is_valid_record(self, record: str) -> bool:
        """Check if record meets size constraints."""
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
        """Process records into appropriately sized batches."""
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
        """Return current processing metrics."""
        metrics = self.metrics.to_dict()
        logger.debug("Current metrics: %s", metrics)
        return metrics


def process_records(records: List[str], 
                   constraints: BatchConstraints = BatchConstraints()) -> List[List[str]]:
    """Convenience function for batch processing."""
    logger.info("Processing records using convenience function")
    processor = BatchProcessor(constraints)
    return list(processor.create_batches(records))
