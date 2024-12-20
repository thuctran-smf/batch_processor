"""
tests/test_batch_processor.py
Unit tests for verifying batch processing functionality.
"""

import logging
import sys
import unittest
from unittest.mock import patch

from src.batch_processor import BatchProcessor, process_records
from src.batch_constraints import BatchConstraints


class TestBatchProcessor(unittest.TestCase):
    """Test suite for BatchProcessor class.
    
    Tests cover validation, constraints, error cases, and general processing.
    """

    def setUp(self):
        """Initialize test environment with default constraints."""
        self.constraints = BatchConstraints()
        self.processor = BatchProcessor(self.constraints)
        # Ensure we don't create log files during testing
        logging.getLogger('src.batch_processor').setLevel(logging.ERROR)

    def test_record_validation(self):
        """Test record size validation including boundary conditions."""
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
        """Test that batches respect size and count constraints."""
        # Test size constraints
        large_records = ["x" * 900_000 for _ in range(6)]
        size_batches = list(self.processor.create_batches(large_records))
        self.assertGreater(len(size_batches), 1)
        
        # Test count constraints
        many_records = ["small" for _ in range(1000)]
        count_batches = list(self.processor.create_batches(many_records))
        for batch in count_batches:
            self.assertLessEqual(len(batch), self.constraints.max_records_per_batch)

    def test_invalid_single_records(self):
        """Test validation of invalid record types."""
        invalid_records = [
            (None, "NoneType"),
            (123, "int"),
            ({}, "dict"),
            ([], "list"),
            (set(), "set")
        ]
        
        for invalid_record, type_name in invalid_records:
            with self.assertRaises(TypeError) as context:
                self.processor.is_valid_record(invalid_record)
            self.assertIn(f"must be string, got {type_name}", str(context.exception))

    def test_invalid_batch_inputs(self):
        """Test validation of invalid batch input types."""
        invalid_batches = [
            (None, "NoneType"),
            (123, "int"),
            ({}, "dict"),
            ("not_a_list", "str")
        ]
        
        for invalid_batch, type_name in invalid_batches:
            with self.assertRaises(TypeError) as context:
                list(self.processor.create_batches(invalid_batch))
            self.assertIn("must be a non-empty list", str(context.exception))

    def test_mixed_types_in_batch(self):
        """Test handling of batches containing mixed types."""
        mixed_batch = [
            "valid_string",
            None,
            "another_valid",
            123,
            "valid_again"
        ]
        
        with self.assertRaises(TypeError) as context:
            list(self.processor.create_batches(mixed_batch))
        self.assertIn("must be string", str(context.exception))

    def test_empty_batch_handling(self):
        """Test processing of empty batch."""
        empty_batch = []
        result = list(self.processor.create_batches(empty_batch))
        
        self.assertEqual(len(result), 0)
        metrics = self.processor.get_metrics()
        self.assertEqual(metrics['records_processed'], 0)
        self.assertEqual(metrics['batches_created'], 0)

    def test_single_record_batch(self):
        """Test processing of single-record batch."""
        single_record = ["test_record"]
        batches = list(self.processor.create_batches(single_record))
        
        self.assertEqual(len(batches), 1)
        self.assertEqual(len(batches[0]), 1)
        self.assertEqual(batches[0][0], "test_record")

    def test_batch_processing(self):
        """Test core batch processing functionality and order preservation."""
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

    def test_metrics_and_logging(self):
        """Test metrics collection and warning logging."""
        with patch('logging.Logger.warning') as mock_warning:
            records = ["test" for _ in range(10)]
            records.append("x" * 2_000_000)
            
            list(self.processor.create_batches(records))
            metrics = self.processor.get_metrics()
            
            self.assertEqual(metrics['records_processed'], 11)
            self.assertEqual(metrics['records_discarded'], 1)
            mock_warning.assert_called_once()

    def test_convenience_function(self):
        """Test the process_records convenience wrapper."""
        records = ["test" for _ in range(10)]
        batches = process_records(records)
        self.assertIsInstance(batches, list)
        self.assertTrue(all(isinstance(batch, list) for batch in batches))


if __name__ == '__main__':
    unittest.main()
