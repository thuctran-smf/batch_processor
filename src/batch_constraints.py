"""
src/batch_constraints.py
Defines size and count constraints for batch processing
"""

from dataclasses import dataclass
from typing import Final

@dataclass(frozen=True)
class BatchConstraints:
    """Defines size and count constraints for batch processing.

        Attributes:
            max_record_size_bytes: Maximum size of a single record
            max_batch_size_bytes: Maximum total size of a batch
            max_records_per_batch: Maximum number of records per batch
    """

    max_record_size_bytes: Final[int] = 1_000_000  # 1 MB
    max_batch_size_bytes: Final[int] = 5_000_000   # 5 MB
    max_records_per_batch: Final[int] = 500
