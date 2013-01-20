"""
Unit tests for PySpark; additional tests are implemented as doctests in
individual modules.
"""
import atexit
import os
import shutil
from tempfile import NamedTemporaryFile
import time
import unittest

from pyspark.context import SparkContext


class TestCheckpoint(unittest.TestCase):

    def setUp(self):
        self.sc = SparkContext('local[4]', 'TestPartitioning', batchSize=2)

    def tearDown(self):
        self.sc.stop()
        # To avoid Akka rebinding to the same port, since it doesn't unbind
        # immediately on shutdown
        self.sc.jvm.System.clearProperty("spark.master.port")

    def test_basic_checkpointing(self):
        checkpointDir = NamedTemporaryFile(delete=False)
        os.unlink(checkpointDir.name)
        self.sc.setCheckpointDir(checkpointDir.name)

        parCollection = self.sc.parallelize([1, 2, 3, 4])
        flatMappedRDD = parCollection.flatMap(lambda x: range(1, x + 1))

        self.assertFalse(flatMappedRDD.isCheckpointed())
        self.assertIsNone(flatMappedRDD.getCheckpointFile())

        flatMappedRDD.checkpoint()
        result = flatMappedRDD.collect()
        time.sleep(1)  # 1 second
        self.assertTrue(flatMappedRDD.isCheckpointed())
        self.assertEqual(flatMappedRDD.collect(), result)
        self.assertEqual(checkpointDir.name,
                         os.path.dirname(flatMappedRDD.getCheckpointFile()))

        atexit.register(lambda: shutil.rmtree(checkpointDir.name))

    def test_checkpoint_and_restore(self):
        checkpointDir = NamedTemporaryFile(delete=False)
        os.unlink(checkpointDir.name)
        self.sc.setCheckpointDir(checkpointDir.name)

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

        atexit.register(lambda: shutil.rmtree(checkpointDir.name))


if __name__ == "__main__":
    unittest.main()
