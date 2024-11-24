"""
demo.py
Demonstration script for batch processor
"""

import logging
from src.batch_processor import BatchProcessor
from src.batch_constraints import BatchConstraints

def create_test_records():
    """Create test records that will demonstrate batch splitting.
    
    Creates:
    - Many medium-sized records to force multiple batches
    - A few invalid records to show error handling
    """
    # Regular records (100KB each)
    medium_record = "x" * 100_000
    regular_records = [f"{medium_record}-{i}" for i in range(750)]  # Will create multiple batches
    
    # Add some invalid records
    oversized_record = "x" * 2_000_000  # 2MB (should be discarded)
    empty_record = ""
    special_chars = "🌟" * 1000
    
    return (regular_records + 
            [oversized_record, empty_record, special_chars])

def main():
    # Configure file handler with DEBUG level
    file_handler = logging.FileHandler('batch_processor.log')
    file_handler.setLevel(logging.DEBUG)
    
    # Configure console handler with INFO level (less verbose)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Configure logger
    logger = logging.getLogger('src.batch_processor')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    print("\n=== Starting Batch Processing Demo ===\n")
    processor = BatchProcessor()
    
    records = create_test_records()
    print(f"Created {len(records)} test records")
    print(f"Each regular record size: ~100KB")
    print(f"Including one oversized record (2MB)")
    print(f"Total records size: ~{len(records) * 100 / 1000:.1f}MB\n")
    
    print("Processing records...")
    batches = list(processor.create_batches(records))
    
    print(f"\n=== Processing Results ===")
    print(f"Number of batches created: {len(batches)}")
    
    # Print summary of each batch
    for i, batch in enumerate(batches, 1):
        print(f"\nBatch {i}:")
        print(f"Number of records: {len(batch)}")
    
    print("\n=== Final Metrics ===")
    metrics = processor.get_metrics()
    print(f"Records processed: {metrics['records_processed']}")
    print(f"Records discarded: {metrics['records_discarded']}")
    print(f"Batches created: {metrics['batches_created']}")
    print(f"Total bytes processed: {metrics['total_bytes_processed']:,}")

if __name__ == "__main__":
    main()
