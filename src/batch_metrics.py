"""
src/batch_metrics.py
Collects and maintains batch processing metrics
"""

from dataclasses import dataclass
from typing import Dict

@dataclass
class BatchMetrics:
    """Collects and maintains processing metrics.

        Attributes:
            records_processed: Total number of records processed
            records_discarded: Number of records that failed validation
            batches_created: Number of batches generated
            total_bytes_processed: Total size of processed records in bytes
    """

    records_processed: int = 0
    records_discarded: int = 0
    batches_created: int = 0
    total_bytes_processed: int = 0
    
    def to_dict(self) -> Dict[str, int]:
        """Convert metrics to dictionary format.

            Returns:
                Dict containing all metric values
        """

        return {
            "records_processed": self.records_processed,
            "records_discarded": self.records_discarded,
            "batches_created": self.batches_created,
            "total_bytes_processed": self.total_bytes_processed
        }
