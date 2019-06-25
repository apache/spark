from pyspark.sql.dataframe import DataFrame


class CoGroupedData(object):

    def __init__(self, gd1, gd2):
        self._gd1 = gd1
        self._gd2 = gd2
        self.sql_ctx = gd1.sql_ctx

    def apply(self, udf):
        all_cols = self._extract_cols(self._gd1) + self._extract_cols(self._gd2)
        udf_column = udf(*all_cols)
        jdf = self._gd1._jgd.flatMapCoGroupsInPandas(self._gd2._jgd, udf_column._jc.expr())
        return DataFrame(jdf, self.sql_ctx)

    @staticmethod
    def _extract_cols(gd):
        df = gd._df
        return [df[col] for col in df.columns]

