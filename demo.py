"""
demo.py
Demonstration script for batch processor
"""

import logging
from src.batch_processor import BatchProcessor
from src.batch_constraints import BatchConstraints

def create_test_records():
    # Create records that will force multiple batches
    # Each record about 100KB (100,000 characters)
    medium_record = "x" * 100_000
    
    # Create 1000 records (this should create multiple batches due to both
    # max_records_per_batch=500 and max_batch_size_bytes=5MB limits)
    return [f"{medium_record}-{i}" for i in range(1000)]

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
    print(f"Each record size: ~100KB")
    print(f"Total records size: ~100MB\n")
    
    print("Processing records...")
    batches = list(processor.create_batches(records))
    
    print(f"\n=== Processing Results ===")
    print(f"Number of batches created: {len(batches)}")
    for i, batch in enumerate(batches, 1):
        print(f"\nBatch {i}:")
        print(f"Number of records: {len(batch)}")
    
    print("\n=== Final Metrics ===")
    print(processor.get_metrics())

if __name__ == "__main__":
    main()
