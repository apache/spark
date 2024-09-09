#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from typing import TYPE_CHECKING, Union


if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from plotly.graph_objs import Figure


class PySparkTopNPlotBase:
    def get_top_n(self, sdf: "DataFrame"):
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        max_rows = int(spark.conf.get("spark.sql.pyspark.plotting.max_rows"))
        pdf = sdf.limit(max_rows + 1).toPandas()

        self.partial = False
        if len(pdf) > max_rows:
            self.partial = True
            pdf = pdf.iloc[:max_rows]

        return pdf


class PySparkSampledPlotBase:
    def get_sampled(self, sdf: "DataFrame"):
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        sample_ratio = spark.conf.get("spark.sql.pyspark.plotting.sample_ratio")
        max_rows = int(spark.conf.get("spark.sql.pyspark.plotting.max_rows"))

        if sample_ratio is None:
            fraction = 1 / (sdf.count() / max_rows)
            fraction = min(1.0, fraction)
        else:
            fraction = float(sample_ratio)

        sampled_sdf = sdf.sample(fraction=fraction)
        pdf = sampled_sdf.toPandas()

        return pdf


class PySparkPlotAccessor:
    plot_data_map = {
        "line": PySparkSampledPlotBase().get_sampled,
    }
    _backends = {}  # type: ignore[var-annotated]

    def __init__(self, data):
        self.data = data

    def __call__(self, kind="line", backend=None, **kwargs):
        plot_backend = PySparkPlotAccessor._get_plot_backend(backend)

        return plot_backend.plot_pyspark(self.data, kind=kind, **kwargs)

    @staticmethod
    def _get_plot_backend(backend=None):
        backend = backend or "plotly"

        if backend in PySparkPlotAccessor._backends:
            return PySparkPlotAccessor._backends[backend]

        if backend == "plotly":
            try:
                import plotly  # noqa: F401
                from pyspark.sql.plot import plotly as module
            except ImportError:
                raise ImportError(
                    "Plotly is required for plotting when the "
                    "default backend 'plotly' is selected."
                ) from None

            PySparkPlotAccessor._backends["plotly"] = module
        else:
            raise ValueError(f"Unsupported backend '{backend}'. Only 'plotly' is supported.")

        return module

    def line(self, x: str, y: Union[str, list[str]], **kwargs) -> "Figure":
        """
        Plot DataFrame as lines.

        Parameters
        ----------
        x : str
            Name of column to use for the horizontal axis.
        y : str or list of str
            Name(s) of the column(s) to use for the vertical axis. Multiple columns can be plotted.
        **kwds : optional
            Additional keyword arguments.

        Returns
        -------
        :class:`plotly.graph_objs.Figure`
        """
        return self(kind="line", x=x, y=y, **kwargs)
