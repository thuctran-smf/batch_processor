"""
src/batch_processor.py
Batch processing system
"""

import logging
from dataclasses import dataclass
from typing import List, Iterator, Final, Dict
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class BatchConstraints:
    max_record_size_bytes: Final[int] = 1_000_000  # 1 MB
    max_batch_size_bytes: Final[int] = 5_000_000   # 5 MB
    max_records_per_batch: Final[int] = 500

@dataclass
class BatchMetrics:
    records_processed: int = 0
    records_discarded: int = 0
    batches_created: int = 0
    total_bytes_processed: int = 0
    
    def to_dict(self) -> Dict[str, int]:
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
        self.constraints: Final[BatchConstraints] = constraints
        self.metrics = BatchMetrics()

    def _get_record_size(self, record: str) -> int:
        """Calculate size of record in bytes."""
        return sys.getsizeof(record.encode('utf-8'))

    def is_valid_record(self, record: str) -> bool:
        """Check if record meets size constraints."""
        record_size = self._get_record_size(record)
        is_valid = record_size <= self.constraints.max_record_size_bytes
        
        if not is_valid:
            logger.warning(f"Record discarded: size {record_size} bytes exceeds limit")
            self.metrics.records_discarded += 1
            
        return is_valid

    def create_batches(self, records: List[str]) -> Iterator[List[str]]:
        """Process records into appropriately sized batches."""
        try:
            current_batch: List[str] = []
            current_batch_size: int = 0
            
            for record in records:
                self.metrics.records_processed += 1
                
                if not self.is_valid_record(record):
                    continue
                    
                record_size = self._get_record_size(record)
                
                if (current_batch_size + record_size > self.constraints.max_batch_size_bytes or
                    len(current_batch) >= self.constraints.max_records_per_batch):
                    if current_batch:
                        self.metrics.batches_created += 1
                        yield current_batch
                    current_batch = []
                    current_batch_size = 0
                
                current_batch.append(record)
                current_batch_size += record_size
                self.metrics.total_bytes_processed += record_size
            
            if current_batch:
                self.metrics.batches_created += 1
                yield current_batch
                
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            raise

    def get_metrics(self) -> Dict[str, int]:
        """Return current processing metrics."""
        return self.metrics.to_dict()

def process_records(records: List[str], 
                   constraints: BatchConstraints = BatchConstraints()) -> List[List[str]]:
    """Convenience function for batch processing."""
    processor = BatchProcessor(constraints)
    return list(processor.create_batches(records))

