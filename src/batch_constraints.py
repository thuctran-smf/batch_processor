"""
src/batch_constraints.py
Defines size and count constraints for batch processing
"""

from dataclasses import dataclass
from typing import Final

@dataclass(frozen=True)
class BatchConstraints:
    """Defines size and count constraints for batch processing."""
    max_record_size_bytes: Final[int] = 1_000_000  # 1 MB
    max_batch_size_bytes: Final[int] = 5_000_000   # 5 MB
    max_records_per_batch: Final[int] = 500
