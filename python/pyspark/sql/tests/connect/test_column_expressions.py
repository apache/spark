from pyspark.sql.tests.connect.utils import PlanOnlyTestFixture

import pyspark.sql.connect as c
import pyspark.sql.connect.plan as p
import pyspark.sql.connect.column as col

import pyspark.sql.connect.functions as fun

class SparkConnectColumnExpressionSuite(PlanOnlyTestFixture):
    def test_simple_column_expressions(self):
        df = c.DataFrame.withPlan(p.Read("table"))

        c1= df.col_name
        assert isinstance(c1, col.ColumnRef)
        c2 = df["col_name"]
        assert isinstance(c2, col.ColumnRef)
        c3 = fun.col("col_name")
        assert isinstance(c3, col.ColumnRef)

        # All Protos should be identical
        cp1 = c1.to_plan(None)
        cp2 = c2.to_plan(None)
        cp3 = c3.to_plan(None)

        assert cp1 is not None
        assert cp1 == cp2 == cp3






if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_column_expressions import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
