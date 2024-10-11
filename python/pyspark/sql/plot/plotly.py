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

from typing import TYPE_CHECKING, Any

from pyspark.errors import PySparkValueError
from pyspark.sql.plot import PySparkPlotAccessor, PySparkBoxPlotBase

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from plotly.graph_objs import Figure


def plot_pyspark(data: "DataFrame", kind: str, **kwargs: Any) -> "Figure":
    import plotly

    if kind == "pie":
        return plot_pie(data, **kwargs)
    if kind == "box":
        return plot_box(data, **kwargs)

    return plotly.plot(PySparkPlotAccessor.plot_data_map[kind](data), kind, **kwargs)


def plot_pie(data: "DataFrame", **kwargs: Any) -> "Figure":
    # TODO(SPARK-49530): Support pie subplots with plotly backend
    from plotly import express

    pdf = PySparkPlotAccessor.plot_data_map["pie"](data)
    x = kwargs.pop("x", None)
    y = kwargs.pop("y", None)
    fig = express.pie(pdf, values=y, names=x, **kwargs)

    return fig


def plot_box(data: "DataFrame", **kwargs: Any) -> "Figure":
    import plotly.graph_objs as go

    # 'whis' isn't actually an argument in plotly (but in matplotlib). But seems like
    # plotly doesn't expose the reach of the whiskers to the beyond the first and
    # third quartiles (?). Looks they use default 1.5.
    whis = kwargs.pop("whis", 1.5)
    # 'precision' is pyspark specific to control precision for approx_percentile
    precision = kwargs.pop("precision", 0.01)
    colnames = kwargs.pop("column", None)
    if isinstance(colnames, str):
        colnames = [colnames]

    # Plotly options
    boxpoints = kwargs.pop("boxpoints", "suspectedoutliers")
    notched = kwargs.pop("notched", False)
    if boxpoints not in ["suspectedoutliers", False]:
        raise PySparkValueError(
            errorClass="UNSUPPORTED_PLOT_BACKEND_PARAM",
            messageParameters={
                "backend": "plotly",
                "param": "boxpoints",
                "value": str(boxpoints),
                "supported_values": ", ".join(["suspectedoutliers", "False"]),
            },
        )
    if notched:
        raise PySparkValueError(
            errorClass="UNSUPPORTED_PLOT_BACKEND_PARAM",
            messageParameters={
                "backend": "plotly",
                "param": "notched",
                "value": str(notched),
                "supported_values": ", ".join(["False"]),
            },
        )

    fig = go.Figure()

    results = PySparkBoxPlotBase.compute_box(
        data,
        colnames,
        whis,
        precision,
        boxpoints is not None,
    )
    assert len(results) == len(colnames)

    for i, colname in enumerate(colnames):
        result = results[i]

        fig.add_trace(
            go.Box(
                x=[i],
                name=colname,
                q1=[result["q1"]],
                median=[result["med"]],
                q3=[result["q3"]],
                mean=[result["mean"]],
                lowerfence=[result["lower_whisker"]],
                upperfence=[result["upper_whisker"]],
                y=[result["fliers"]] if result["fliers"] else None,
                boxpoints=boxpoints,
                notched=notched,
                **kwargs,
            )
        )

    fig["layout"]["yaxis"]["title"] = "value"
    return fig
