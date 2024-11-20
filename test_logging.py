from src.batch_processor import BatchProcessor, BatchConstraints

# Create test data
normal_record = "test"
oversized_record = "x" * 2_000_000
records = [normal_record, oversized_record, normal_record]

# Process records
processor = BatchProcessor()
list(processor.create_batches(records))
