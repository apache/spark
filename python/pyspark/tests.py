"""
Unit tests for PySpark; additional tests are implemented as doctests in
individual modules.
"""
import os
import shutil
from tempfile import NamedTemporaryFile
import time
import unittest

from pyspark.context import SparkContext


class TestCheckpoint(unittest.TestCase):

    def setUp(self):
        self.sc = SparkContext('local[4]', 'TestPartitioning', batchSize=2)
        self.checkpointDir = NamedTemporaryFile(delete=False)
        os.unlink(self.checkpointDir.name)
        self.sc.setCheckpointDir(self.checkpointDir.name)

    def tearDown(self):
        self.sc.stop()
        # To avoid Akka rebinding to the same port, since it doesn't unbind
        # immediately on shutdown
        self.sc.jvm.System.clearProperty("spark.master.port")
        shutil.rmtree(self.checkpointDir.name)

    def test_basic_checkpointing(self):
        parCollection = self.sc.parallelize([1, 2, 3, 4])
        flatMappedRDD = parCollection.flatMap(lambda x: range(1, x + 1))

        self.assertFalse(flatMappedRDD.isCheckpointed())
        self.assertIsNone(flatMappedRDD.getCheckpointFile())

        flatMappedRDD.checkpoint()
        result = flatMappedRDD.collect()
        time.sleep(1)  # 1 second
        self.assertTrue(flatMappedRDD.isCheckpointed())
        self.assertEqual(flatMappedRDD.collect(), result)
        self.assertEqual(self.checkpointDir.name,
                         os.path.dirname(flatMappedRDD.getCheckpointFile()))

    def test_checkpoint_and_restore(self):
        parCollection = self.sc.parallelize([1, 2, 3, 4])
        flatMappedRDD = parCollection.flatMap(lambda x: [x])

        self.assertFalse(flatMappedRDD.isCheckpointed())
        self.assertIsNone(flatMappedRDD.getCheckpointFile())

        flatMappedRDD.checkpoint()
        flatMappedRDD.count()  # forces a checkpoint to be computed
        time.sleep(1)  # 1 second

        self.assertIsNotNone(flatMappedRDD.getCheckpointFile())
        recovered = self.sc._checkpointFile(flatMappedRDD.getCheckpointFile())
        self.assertEquals([1, 2, 3, 4], recovered.collect())


if __name__ == "__main__":
    unittest.main()
