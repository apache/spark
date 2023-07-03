from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.sql.tests.test_utils import UtilsTestsMixin


class ConnectUtilsTests(ReusedConnectTestCase, UtilsTestsMixin):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_utils import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
