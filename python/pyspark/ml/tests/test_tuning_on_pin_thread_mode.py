import os
import time
import unittest

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, TrainValidationSplit


class TestBackgroundSparkJobsCanceledCorrectly(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.old_pin_thread = os.environ.get("PYSPARK_PIN_THREAD")
        os.environ["PYSPARK_PIN_THREAD"] = "true"
        cls.sc = SparkContext('local[4]', cls.__name__, conf=SparkConf())
        cls.spark = SparkSession(cls.sc)

        cls.parallelism = 3
        trial_status = ["unstart"] * (cls.parallelism * 2)

        cls.dataset = cls.spark.createDataFrame(
            [
                (10, 10.0),
                (50, 50.0),
                (100, 100.0),
                (500, 500.0)] * 100,
            ["feature", "label"])

        class TestingEstimator(LogisticRegression):
            def _fit(self, dataset):
                index = self.getMaxIter()
                trial_status[index] = "running"

                if index == 0:
                    trial_status[index] = "failed"
                    raise RuntimeError()

                def mapper(pid, iterator):
                    if pid == 0:
                        time.sleep(3)
                        yield 1

                try:
                    dataset.rdd.mapPartitionsWithIndex(mapper).count()
                except:
                    trial_status[index] = "canceled"
                    raise RuntimeError()

                trial_status[index] = "finished"
                raise RuntimeError()

        cls.TestingEstimator = TestingEstimator
        cls.trial_status = trial_status

    @classmethod
    def resetTrialStatus(cls):
        for i in range(len(cls.trial_status)):
            cls.trial_status[i] = "unstart"

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        if cls.old_pin_thread is not None:
            os.environ["PYSPARK_PIN_THREAD"] = cls.old_pin_thread
        else:
            del os.environ["PYSPARK_PIN_THREAD"]

    def test_fit_raise_error_and_then_background_jobs_canceled(self):
        # This is a test for ML-14996 fix.
        lr = self.TestingEstimator()
        evaluator = BinaryClassificationEvaluator()

        grid = (ParamGridBuilder()
                .addGrid(lr.maxIter, list(range(self.parallelism * 2)))
                .build())

        for EstimatorCls in [CrossValidator, TrainValidationSplit]:
            est = EstimatorCls(estimator=lr,
                               estimatorParamMaps=grid,
                               evaluator=evaluator,
                               parallelism=self.parallelism)
            self.resetTrialStatus()

            try:
                est.fit(self.dataset)
            except:
                pass

            time.sleep(4)
            n = self.parallelism
            self.assertEqual(self.trial_status[0], "failed")
            self.assertEqual(self.trial_status[1: n], ["canceled"] * (n - 1))
            self.assertIn(self.trial_status[n], ["canceled", "unstart"])
            self.assertEqual(self.trial_status[n + 1: n * 2], ["unstart"] * (n - 1))


if __name__ == "__main__":
    from pyspark.ml.tests.test_tuning_on_pin_thread_mode import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
