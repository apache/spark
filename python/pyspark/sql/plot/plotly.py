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

import inspect
from typing import TYPE_CHECKING, Any, List, Optional, Union

from pyspark.errors import PySparkTypeError, PySparkValueError
from pyspark.sql.plot import (
    PySparkPlotAccessor,
    PySparkBoxPlotBase,
    PySparkKdePlotBase,
    PySparkHistogramPlotBase,
)
from pyspark.sql.types import NumericType

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from plotly.graph_objs import Figure


def plot_pyspark(data: "DataFrame", kind: str, **kwargs: Any) -> "Figure":
    import plotly

    if kind == "pie":
        return plot_pie(data, **kwargs)
    if kind == "box":
        return plot_box(data, **kwargs)
    if kind == "kde" or kind == "density":
        return plot_kde(data, **kwargs)
    if kind == "hist":
        return plot_histogram(data, **kwargs)

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
    colnames = process_column_param(kwargs.pop("column", None), data)

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
    assert len(results) == len(colnames)  # type: ignore

    for i, colname in enumerate(colnames):
        result = results[i]  # type: ignore

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


def plot_kde(data: "DataFrame", **kwargs: Any) -> "Figure":
    from pyspark.sql.pandas.utils import require_minimum_pandas_version

    require_minimum_pandas_version()

    import pandas as pd
    from plotly import express

    if "color" not in kwargs:
        kwargs["color"] = "names"

    bw_method = kwargs.pop("bw_method", None)
    colnames = process_column_param(kwargs.pop("column", None), data)
    ind = PySparkKdePlotBase.get_ind(data.select(*colnames), kwargs.pop("ind", None))

    kde_cols = [
        PySparkKdePlotBase.compute_kde_col(
            input_col=data[col_name],
            ind=ind,
            bw_method=bw_method,
        ).alias(f"kde_{i}")
        for i, col_name in enumerate(colnames)
    ]
    kde_results = data.select(*kde_cols).first()
    pdf = pd.concat(
        [
            pd.DataFrame(  # type: ignore
                {
                    "Density": kde_result,
                    "names": col_name,
                    "index": ind,
                }
            )
            for col_name, kde_result in zip(colnames, list(kde_results))  # type: ignore[arg-type]
        ]
    )
    fig = express.line(pdf, x="index", y="Density", **kwargs)
    fig["layout"]["xaxis"]["title"] = None
    return fig


def plot_histogram(data: "DataFrame", **kwargs: Any) -> "Figure":
    import plotly.graph_objs as go

    bins = kwargs.get("bins", 10)
    colnames = process_column_param(kwargs.pop("column", None), data)
    numeric_data = data.select(*colnames)
    bins = PySparkHistogramPlotBase.get_bins(numeric_data, bins)
    assert len(bins) > 2, "the number of buckets must be higher than 2."
    output_series = PySparkHistogramPlotBase.compute_hist(numeric_data, bins)
    prev = float("%.9f" % bins[0])  # to make it prettier, truncate.
    text_bins = []
    for b in bins[1:]:
        norm_b = float("%.9f" % b)
        text_bins.append("[%s, %s)" % (prev, norm_b))
        prev = norm_b
    text_bins[-1] = text_bins[-1][:-1] + "]"  # replace ) to ] for the last bucket.

    bins = 0.5 * (bins[:-1] + bins[1:])
    output_series = list(output_series)
    bars = []
    for series in output_series:
        bars.append(
            go.Bar(
                x=bins,
                y=series,
                name=series.name,
                text=text_bins,
                hovertemplate=("variable=" + str(series.name) + "<br>value=%{text}<br>count=%{y}"),
            )
        )

    layout_keys = inspect.signature(go.Layout).parameters.keys()
    layout_kwargs = {k: v for k, v in kwargs.items() if k in layout_keys}

    fig = go.Figure(data=bars, layout=go.Layout(**layout_kwargs))
    fig["layout"]["barmode"] = "stack"
    fig["layout"]["xaxis"]["title"] = "value"
    fig["layout"]["yaxis"]["title"] = "count"
    return fig


def process_column_param(column: Optional[Union[str, List[str]]], data: "DataFrame") -> List[str]:
    """
    Processes the provided column parameter for a DataFrame.
    - If `column` is None, returns a list of numeric columns from the DataFrame.
    - If `column` is a string, converts it to a list first.
    - If `column` is a list, it checks if all specified columns exist in the DataFrame
      and are of NumericType.
    - Raises a PySparkTypeError if any column in the list is not present in the DataFrame
      or is not of NumericType.
    """
    if column is None:
        return [
            field.name for field in data.schema.fields if isinstance(field.dataType, NumericType)
        ]
    if isinstance(column, str):
        column = [column]

    for col in column:
        field = next((f for f in data.schema.fields if f.name == col), None)
        if not field or not isinstance(field.dataType, NumericType):
            raise PySparkTypeError(
                errorClass="PLOT_INVALID_TYPE_COLUMN",
                messageParameters={
                    "col_name": col,
                    "valid_types": NumericType.__name__,
                    "col_type": field.dataType.__class__.__name__ if field else "None",
                },
            )
    return column
