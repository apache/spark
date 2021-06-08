import decimal as d

import pandas as pd

from pyspark.pandas.data_type_ops.num_ops import DecimalOps
from pyspark import pandas as ps
from pyspark.pandas.tests.data_type_ops.testing_utils import TestCasesUtils
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class DecimalOpsTest(PandasOnSparkTestCase, TestCasesUtils):
    @property
    def decimal_pser(self):
        return pd.Series([d.Decimal(1.0), d.Decimal(2.0)])

    @property
    def decimal_psser(self):
        return ps.from_pandas(self.decimal_pser)

    def test_datatype_ops(self):
        psser = self.decimal_psser
        self.assertIsInstance(psser._dtype_op, DecimalOps)
        self.assertEqual(psser._dtype_op.pretty_name, "decimal")
