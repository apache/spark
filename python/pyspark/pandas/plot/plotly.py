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
from typing import TYPE_CHECKING, Union

import pandas as pd

from pyspark.pandas.plot import (
    HistogramPlotBase,
    name_like_string,
    PandasOnSparkPlotAccessor,
    BoxPlotBase,
    KdePlotBase,
)

if TYPE_CHECKING:
    import pyspark.pandas as ps


def plot_pandas_on_spark(data: Union["ps.DataFrame", "ps.Series"], kind: str, **kwargs):
    import plotly

    # pandas-on-Spark specific plots
    if kind == "pie":
        return plot_pie(data, **kwargs)
    if kind == "hist":
        return plot_histogram(data, **kwargs)
    if kind == "box":
        return plot_box(data, **kwargs)
    if kind == "kde" or kind == "density":
        return plot_kde(data, **kwargs)

    # Other plots.
    return plotly.plot(PandasOnSparkPlotAccessor.pandas_plot_data_map[kind](data), kind, **kwargs)


def plot_pie(data: Union["ps.DataFrame", "ps.Series"], **kwargs):
    from plotly import express

    data = PandasOnSparkPlotAccessor.pandas_plot_data_map["pie"](data)

    if isinstance(data, pd.Series):
        pdf = data.to_frame()
        return express.pie(pdf, values=pdf.columns[0], names=pdf.index, **kwargs)
    elif isinstance(data, pd.DataFrame):
        values = kwargs.pop("y", None)
        default_names = None
        if values is not None:
            default_names = data.index

        return express.pie(
            data,
            values=kwargs.pop("values", values),
            names=kwargs.pop("names", default_names),
            **kwargs,
        )
    else:
        raise RuntimeError("Unexpected type: [%s]" % type(data))


def plot_histogram(data: Union["ps.DataFrame", "ps.Series"], **kwargs):
    import plotly.graph_objs as go
    import pyspark.pandas as ps

    bins = kwargs.get("bins", 10)
    y = kwargs.get("y")
    if y and isinstance(data, ps.DataFrame):
        # Note that the results here are matched with matplotlib. x and y
        # handling is different from pandas' plotly output.
        data = data[y]
    psdf, bins = HistogramPlotBase.prepare_hist_data(data, bins)
    assert len(bins) > 2, "the number of buckets must be higher than 2."
    output_series = HistogramPlotBase.compute_hist(psdf, bins)
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
                name=name_like_string(series.name),
                text=text_bins,
                hovertemplate=(
                    "variable=" + name_like_string(series.name) + "<br>value=%{text}<br>count=%{y}"
                ),
            )
        )

    layout_keys = inspect.signature(go.Layout).parameters.keys()
    layout_kwargs = {k: v for k, v in kwargs.items() if k in layout_keys}

    fig = go.Figure(data=bars, layout=go.Layout(**layout_kwargs))
    fig["layout"]["barmode"] = "stack"
    fig["layout"]["xaxis"]["title"] = "value"
    fig["layout"]["yaxis"]["title"] = "count"
    return fig


def plot_box(data: Union["ps.DataFrame", "ps.Series"], **kwargs):
    import plotly.graph_objs as go
    import pyspark.pandas as ps
    from pyspark.sql.types import NumericType

    # 'whis' isn't actually an argument in plotly (but in matplotlib). But seems like
    # plotly doesn't expose the reach of the whiskers to the beyond the first and
    # third quartiles (?). Looks they use default 1.5.
    whis = kwargs.pop("whis", 1.5)
    # 'precision' is pandas-on-Spark specific to control precision for approx_percentile
    precision = kwargs.pop("precision", 0.01)

    # Plotly options
    boxpoints = kwargs.pop("boxpoints", "suspectedoutliers")
    notched = kwargs.pop("notched", False)
    if boxpoints not in ["suspectedoutliers", False]:
        raise ValueError(
            "plotly plotting backend does not support 'boxpoints' set to '%s'. "
            "Set to 'suspectedoutliers' or False." % boxpoints
        )
    if notched:
        raise ValueError(
            "plotly plotting backend does not support 'notched' set to '%s'. "
            "Set to False." % notched
        )

    fig = go.Figure()
    if isinstance(data, ps.Series):
        colname = name_like_string(data.name)
        spark_column_name = data._internal.spark_column_name_for(data._column_label)

        # Computes mean, median, Q1 and Q3 with approx_percentile and precision
        col_stats, col_fences = BoxPlotBase.compute_stats(data, spark_column_name, whis, precision)

        # Creates a column to flag rows as outliers or not
        outliers = BoxPlotBase.outliers(data, spark_column_name, *col_fences)

        # Computes min and max values of non-outliers - the whiskers
        whiskers = BoxPlotBase.calc_whiskers(spark_column_name, outliers)

        fliers = None
        if boxpoints:
            fliers = BoxPlotBase.get_fliers(spark_column_name, outliers, whiskers[0])
            fliers = [fliers] if len(fliers) > 0 else None

        fig.add_trace(
            go.Box(
                name=colname,
                q1=[col_stats["q1"]],
                median=[col_stats["med"]],
                q3=[col_stats["q3"]],
                mean=[col_stats["mean"]],
                lowerfence=[whiskers[0]],
                upperfence=[whiskers[1]],
                y=fliers,
                boxpoints=boxpoints,
                notched=notched,
                **kwargs,  # this is for workarounds. Box takes different options from express.box.
            )
        )
        fig["layout"]["xaxis"]["title"] = colname

    else:
        numeric_column_names = []
        for column_label in data._internal.column_labels:
            if isinstance(data._internal.spark_type_for(column_label), NumericType):
                numeric_column_names.append(name_like_string(column_label))

        # Computes mean, median, Q1 and Q3 with approx_percentile and precision
        multicol_stats = BoxPlotBase.compute_multicol_stats(
            data, numeric_column_names, whis, precision
        )

        # Creates a column to flag rows as outliers or not
        outliers = BoxPlotBase.multicol_outliers(data, multicol_stats)

        # Computes min and max values of non-outliers - the whiskers
        whiskers = BoxPlotBase.calc_multicol_whiskers(numeric_column_names, outliers)

        fliers = None
        if boxpoints:
            fliers = BoxPlotBase.get_multicol_fliers(numeric_column_names, outliers, whiskers)

        i = 0
        for colname in numeric_column_names:
            col_stats = multicol_stats[colname]
            col_whiskers = whiskers[colname]

            col_fliers = None
            if fliers is not None and colname in fliers and len(fliers[colname]) > 0:
                col_fliers = [fliers[colname]]

            fig.add_trace(
                go.Box(
                    x=[i],
                    name=colname,
                    q1=[col_stats["q1"]],
                    median=[col_stats["med"]],
                    q3=[col_stats["q3"]],
                    mean=[col_stats["mean"]],
                    lowerfence=[col_whiskers["min"]],
                    upperfence=[col_whiskers["max"]],
                    y=col_fliers,
                    boxpoints=boxpoints,
                    notched=notched,
                    **kwargs,
                )
            )
            i += 1

    fig["layout"]["yaxis"]["title"] = "value"
    return fig


def plot_kde(data: Union["ps.DataFrame", "ps.Series"], **kwargs):
    from plotly import express
    import pyspark.pandas as ps

    if isinstance(data, ps.DataFrame) and "color" not in kwargs:
        kwargs["color"] = "names"

    psdf = KdePlotBase.prepare_kde_data(data)
    sdf = psdf._internal.spark_frame
    data_columns = psdf._internal.data_spark_columns
    ind = KdePlotBase.get_ind(sdf.select(*data_columns), kwargs.pop("ind", None))
    bw_method = kwargs.pop("bw_method", None)

    kde_cols = [
        KdePlotBase.compute_kde_col(
            input_col=psdf._internal.spark_column_for(label),
            ind=ind,
            bw_method=bw_method,
        ).alias(f"kde_{i}")
        for i, label in enumerate(psdf._internal.column_labels)
    ]
    kde_results = sdf.select(*kde_cols).first()

    pdf = pd.concat(
        [
            pd.DataFrame(
                {
                    "Density": kde_result,
                    "names": name_like_string(label),
                    "index": ind,
                }
            )
            for label, kde_result in zip(psdf._internal.column_labels, list(kde_results))
        ]
    )

    fig = express.line(pdf, x="index", y="Density", **kwargs)
    fig["layout"]["xaxis"]["title"] = None
    return fig
