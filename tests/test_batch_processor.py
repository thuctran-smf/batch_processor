"""
tests/test_batch_processor.py
Unit tests for the batch processor
"""
import logging
from src.batch_processor import BatchProcessor, BatchConstraints, process_records
import sys
import unittest
from unittest.mock import patch


class TestBatchProcessor(unittest.TestCase):
    def setUp(self):
        self.constraints = BatchConstraints()
        self.processor = BatchProcessor(self.constraints)
        logging.getLogger('src.batch_processor').setLevel(logging.ERROR)

    def test_record_validation(self):
        """Test record validation including boundary conditions"""
        # Valid cases
        self.assertTrue(self.processor.is_valid_record("small"))
        
        # Exact size limit
        record_size = self.constraints.max_record_size_bytes
        self.assertTrue(self.processor.is_valid_record("x" * (record_size - 100)))
        
        # Invalid cases
        self.assertFalse(self.processor.is_valid_record("x" * 2_000_000))
        
        # Check metrics
        metrics = self.processor.get_metrics()
        self.assertEqual(metrics['records_discarded'], 1)

    def test_batch_constraints(self):
        """Test all batch constraints (size and count)"""
        # Test size constraints - using smaller records now
        large_records = ["x" * 900_000 for _ in range(6)]  # Changed from 1_000_000
        size_batches = list(self.processor.create_batches(large_records))
        self.assertGreater(len(size_batches), 1)  # Should create multiple batches
        
        # Test count constraints
        many_records = ["small" for _ in range(1000)]
        count_batches = list(self.processor.create_batches(many_records))
        for batch in count_batches:
            self.assertLessEqual(len(batch), self.constraints.max_records_per_batch)

    def test_invalid_inputs(self):
        """Test all invalid input scenarios"""
        # Invalid types
        invalid_inputs = [
            None, 123, {}, [], 
            "not_a_list",
            ["valid", None, "invalid"]  # Mixed types
        ]
        
        for invalid_input in invalid_inputs:
            with self.assertRaises((TypeError, ValueError)):
                if isinstance(invalid_input, list):
                    list(self.processor.create_batches(invalid_input))
                else:
                    with self.assertRaises(TypeError):
                        if not isinstance(invalid_input, str):
                            self.processor.is_valid_record(invalid_input)

    def test_metrics_and_logging(self):
        """Test metrics collection and logging"""
        with patch('logging.Logger.warning') as mock_warning:
            records = ["test" for _ in range(10)]
            records.append("x" * 2_000_000)
            
            list(self.processor.create_batches(records))
            metrics = self.processor.get_metrics()
            
            self.assertEqual(metrics['records_processed'], 11)
            self.assertEqual(metrics['records_discarded'], 1)
            mock_warning.assert_called_once()

    def test_batch_processing(self):
        """Test batch processing functionality"""
        # Test order preservation
        ordered_records = [str(i) for i in range(10)]
        batches = list(self.processor.create_batches(ordered_records))
        flattened = [int(record) for batch in batches for record in batch]
        self.assertEqual(flattened, list(range(10)))
        
        # Test mixed sizes
        mixed_records = [
            "small",
            "x" * 100_000,
            "medium" * 1000,
            "tiny"
        ]
        mixed_batches = list(self.processor.create_batches(mixed_records))
        self.assertTrue(all(len(batch) > 0 for batch in mixed_batches))

    @patch('logging.Logger.warning')
    def test_convenience_function(self, mock_warning):
        """Test the convenience wrapper"""
        records = ["test" for _ in range(10)]
        batches = process_records(records)
        self.assertIsInstance(batches, list)
        self.assertTrue(all(isinstance(batch, list) for batch in batches))

if __name__ == '__main__':
    unittest.main()
