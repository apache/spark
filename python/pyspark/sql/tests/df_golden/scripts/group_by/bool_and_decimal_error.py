# input type checking Decimal

from decimal import Decimal
from pyspark.sql.functions import bool_and, lit

df = spark.table("test_agg").select(bool_and(lit(Decimal("1.0"))))
