"""
tests/test_batch_processor.py
Unit tests for the batch processor
"""

import unittest
from unittest.mock import patch
import logging
from src.batch_processor import BatchProcessor, BatchConstraints, process_records

class TestBatchProcessor(unittest.TestCase):
    def setUp(self):
        self.constraints = BatchConstraints()
        self.processor = BatchProcessor(self.constraints)
        
    def test_record_validation(self):
        """Test record size validation"""
        valid_record = "small"
        invalid_record = "x" * 2_000_000  # ~2MB
        
        self.assertTrue(self.processor.is_valid_record(valid_record))
        self.assertFalse(self.processor.is_valid_record(invalid_record))
        
    def test_batch_size_constraints(self):
        """Test batch size limits are respected"""
        record_size = 1_000_000
        records = ["x" * (record_size - 100) for _ in range(6)]
        
        batches = list(self.processor.create_batches(records))
        metrics = self.processor.get_metrics()
        
        self.assertGreater(len(batches), 1)
        self.assertEqual(metrics['batches_created'], len(batches))
        
    def test_record_count_limits(self):
        """Test record count limits are respected"""
        records = ["small" for _ in range(1000)]
        
        batches = list(self.processor.create_batches(records))
        
        for batch in batches:
            self.assertLessEqual(len(batch), self.constraints.max_records_per_batch)
            
    def test_metrics_collection(self):
        """Test metrics are collected correctly"""
        records = ["test" for _ in range(10)]
        records.append("x" * 2_000_000)  # Add invalid record
        
        list(self.processor.create_batches(records))
        metrics = self.processor.get_metrics()
        
        self.assertEqual(metrics['records_processed'], 11)
        self.assertEqual(metrics['records_discarded'], 1)
        self.assertGreater(metrics['total_bytes_processed'], 0)
        
    @patch('logging.Logger.warning')
    def test_logging(self, mock_warning):
        """Test logging functionality"""
        invalid_record = "x" * 2_000_000
        self.processor.is_valid_record(invalid_record)
        
        mock_warning.assert_called_once()
        
    def test_convenience_function(self):
        """Test process_records convenience function"""
        records = ["test" for _ in range(10)]
        
        batches = process_records(records)
        self.assertIsInstance(batches, list)
        self.assertTrue(all(isinstance(batch, list) for batch in batches))

if __name__ == '__main__':
    unittest.main()
