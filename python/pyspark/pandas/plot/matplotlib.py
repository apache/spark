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

from typing import final

from pyspark.loose_version import LooseVersion

import matplotlib as mat
import numpy as np
from matplotlib.axes._base import _process_plot_format  # type: ignore[attr-defined]
from matplotlib.figure import Figure
from pandas.core.dtypes.inference import is_list_like
from pandas.io.formats.printing import pprint_thing
from pandas.plotting._matplotlib import (  # type: ignore[attr-defined]
    BarPlot as PandasBarPlot,
    BoxPlot as PandasBoxPlot,
    HistPlot as PandasHistPlot,
    PiePlot as PandasPiePlot,
    AreaPlot as PandasAreaPlot,
    LinePlot as PandasLinePlot,
    BarhPlot as PandasBarhPlot,
    ScatterPlot as PandasScatterPlot,
    KdePlot as PandasKdePlot,
)
from pandas.plotting._core import PlotAccessor
from pandas.plotting._matplotlib.core import MPLPlot as PandasMPLPlot

from pyspark.pandas.plot import (
    TopNPlotBase,
    SampledPlotBase,
    HistogramPlotBase,
    BoxPlotBase,
    unsupported_function,
    KdePlotBase,
)
from pyspark.pandas.series import Series, first_series

_all_kinds = PlotAccessor._all_kinds  # type: ignore[attr-defined]


def _set_ticklabels(ax, labels, is_vertical, **kwargs) -> None:
    """Set the tick labels of a given axis.

    Due to https://github.com/matplotlib/matplotlib/pull/17266, we need to handle the
    case of repeated ticks (due to `FixedLocator`) and thus we duplicate the number of
    labels.
    """
    ticks = ax.get_xticks() if is_vertical else ax.get_yticks()
    if len(ticks) != len(labels):
        i, remainder = divmod(len(ticks), len(labels))
        assert remainder == 0, remainder
        labels *= i
    if is_vertical:
        ax.set_xticklabels(labels, **kwargs)
    else:
        ax.set_yticklabels(labels, **kwargs)


class PandasOnSparkBarPlot(PandasBarPlot, TopNPlotBase):
    _kind = "bar"

    def __init__(self, data, **kwargs):
        super().__init__(self.get_top_n(data), **kwargs)

    def _plot(self, ax, x, y, w, start=0, log=False, **kwds):
        self.set_result_text(ax)
        return ax.bar(x, y, w, bottom=start, log=log, **kwds)


class PandasOnSparkBoxPlot(PandasBoxPlot, BoxPlotBase):
    _kind = "box"

    def boxplot(
        self,
        ax,
        bxpstats,
        notch=None,
        sym=None,
        vert=None,
        whis=None,
        positions=None,
        widths=None,
        patch_artist=None,
        bootstrap=None,
        usermedians=None,
        conf_intervals=None,
        meanline=None,
        showmeans=None,
        showcaps=None,
        showbox=None,
        showfliers=None,
        boxprops=None,
        labels=None,
        flierprops=None,
        medianprops=None,
        meanprops=None,
        capprops=None,
        whiskerprops=None,
        manage_ticks=None,
        # manage_xticks is for compatibility of matplotlib < 3.1.0.
        # Remove this when minimum version is 3.0.0
        manage_xticks=None,
        autorange=False,
        zorder=None,
        precision=None,
    ):
        def update_dict(dictionary, rc_name, properties):
            """Loads properties in the dictionary from rc file if not already
            in the dictionary"""
            rc_str = "boxplot.{0}.{1}"
            if dictionary is None:
                dictionary = dict()
            for prop_dict in properties:
                dictionary.setdefault(prop_dict, mat.rcParams[rc_str.format(rc_name, prop_dict)])
            return dictionary

        # Common property dictionaries loading from rc
        flier_props = [
            "color",
            "marker",
            "markerfacecolor",
            "markeredgecolor",
            "markersize",
            "linestyle",
            "linewidth",
        ]
        default_props = ["color", "linewidth", "linestyle"]

        boxprops = update_dict(boxprops, "boxprops", default_props)
        whiskerprops = update_dict(whiskerprops, "whiskerprops", default_props)
        capprops = update_dict(capprops, "capprops", default_props)
        medianprops = update_dict(medianprops, "medianprops", default_props)
        meanprops = update_dict(meanprops, "meanprops", default_props)
        flierprops = update_dict(flierprops, "flierprops", flier_props)

        if patch_artist:
            boxprops["linestyle"] = "solid"
            boxprops["edgecolor"] = boxprops.pop("color")

        # if non-default sym value, put it into the flier dictionary
        # the logic for providing the default symbol ('b+') now lives
        # in bxp in the initial value of final_flierprops
        # handle all of the `sym` related logic here so we only have to pass
        # on the flierprops dict.
        if sym is not None:
            # no-flier case, which should really be done with
            # 'showfliers=False' but none-the-less deal with it to keep back
            # compatibility
            if sym == "":
                # blow away existing dict and make one for invisible markers
                flierprops = dict(linestyle="none", marker="", color="none")
                # turn the fliers off just to be safe
                showfliers = False
            # now process the symbol string
            else:
                # process the symbol string
                # discarded linestyle
                _, marker, color = _process_plot_format(sym)
                # if we have a marker, use it
                if marker is not None:
                    flierprops["marker"] = marker
                # if we have a color, use it
                if color is not None:
                    # assume that if color is passed in the user want
                    # filled symbol, if the users want more control use
                    # flierprops
                    flierprops["color"] = color
                    flierprops["markerfacecolor"] = color
                    flierprops["markeredgecolor"] = color

        # replace medians if necessary:
        if usermedians is not None:
            if len(np.ravel(usermedians)) != len(bxpstats) or np.shape(usermedians)[0] != len(
                bxpstats
            ):
                raise ValueError("usermedians length not compatible with x")
            else:
                # reassign medians as necessary
                for stats, med in zip(bxpstats, usermedians):
                    if med is not None:
                        stats["med"] = med

        if conf_intervals is not None:
            if np.shape(conf_intervals)[0] != len(bxpstats):
                err_mess = "conf_intervals length not compatible with x"
                raise ValueError(err_mess)
            else:
                for stats, ci in zip(bxpstats, conf_intervals):
                    if ci is not None:
                        if len(ci) != 2:
                            raise ValueError("each confidence interval must " "have two values")
                        else:
                            if ci[0] is not None:
                                stats["cilo"] = ci[0]
                            if ci[1] is not None:
                                stats["cihi"] = ci[1]

        should_manage_ticks = True
        if manage_xticks is not None:
            should_manage_ticks = manage_xticks
        if manage_ticks is not None:
            should_manage_ticks = manage_ticks

        if LooseVersion(mat.__version__) < LooseVersion("3.1.0"):
            extra_args = {"manage_xticks": should_manage_ticks}
        else:
            extra_args = {"manage_ticks": should_manage_ticks}

        artists = ax.bxp(
            bxpstats,
            positions=positions,
            widths=widths,
            vert=vert,
            patch_artist=patch_artist,
            shownotches=notch,
            showmeans=showmeans,
            showcaps=showcaps,
            showbox=showbox,
            boxprops=boxprops,
            flierprops=flierprops,
            medianprops=medianprops,
            meanprops=meanprops,
            meanline=meanline,
            showfliers=showfliers,
            capprops=capprops,
            whiskerprops=whiskerprops,
            zorder=zorder,
            **extra_args,
        )
        return artists

    def _plot(self, ax, bxpstats, column_num=None, return_type="axes", **kwds):
        bp = self.boxplot(ax, bxpstats, **kwds)

        if return_type == "dict":
            return bp, bp
        elif return_type == "both":
            return self.BP(ax=ax, lines=bp), bp
        else:
            return ax, bp

    @final
    def _ensure_frame(self, data):
        if isinstance(data, Series):
            label = self.label
            if label is None and data.name is None:
                label = ""
            if label is None:
                data = data.to_frame()
            else:
                data = data.to_frame(name=label)
        return data

    def _compute_plot_data(self):
        data = self.data
        data = first_series(data) if not isinstance(data, Series) else data
        colname = data.name
        spark_column_name = data._internal.spark_column_name_for(data._column_label)

        # Updates all props with the rc defaults from matplotlib
        self.kwds.update(PandasOnSparkBoxPlot.rc_defaults(**self.kwds))

        # Gets some important kwds
        showfliers = self.kwds.get("showfliers", False)
        whis = self.kwds.get("whis", 1.5)
        labels = self.kwds.get("labels", [colname])

        # This one is pandas-on-Spark specific to control precision for approx_percentile
        precision = self.kwds.get("precision", 0.01)

        results = BoxPlotBase.compute_box(
            data._psdf._internal.resolved_copy.spark_frame,
            [spark_column_name],
            whis,
            precision,
            showfliers,
        )
        assert len(results) == 1
        result = results[0]

        # Builds bxpstats dict
        stats = []
        item = {
            "mean": result["mean"],
            "med": result["med"],
            "q1": result["q1"],
            "q3": result["q3"],
            "whislo": result["lower_whisker"],
            "whishi": result["upper_whisker"],
            "fliers": result["fliers"] if result["fliers"] else [],
            "label": labels[0],
        }
        stats.append(item)

        self.data = {labels[0]: stats}

    def _make_plot(self, fig: Figure):
        bxpstats = list(self.data.values())[0]
        ax = self._get_ax(0)
        kwds = self.kwds.copy()

        for stats in bxpstats:
            if len(stats["fliers"]) > 1000:
                stats["fliers"] = stats["fliers"][:1000]
                ax.text(
                    1,
                    1,
                    "showing top 1,000 fliers only",
                    size=6,
                    ha="right",
                    va="bottom",
                    transform=ax.transAxes,
                )

        ret, bp = self._plot(ax, bxpstats, column_num=0, return_type=self.return_type, **kwds)
        self.maybe_color_bp(bp)
        self._return_obj = ret

        labels = [lbl for lbl, _ in self.data.items()]
        labels = [pprint_thing(lbl) for lbl in labels]
        if not self.use_index:
            labels = [pprint_thing(key) for key in range(len(labels))]
        _set_ticklabels(ax, labels, self.orientation == "vertical")

    @staticmethod
    def rc_defaults(
        notch=None,
        vert=None,
        whis=None,
        patch_artist=None,
        bootstrap=None,
        meanline=None,
        showmeans=None,
        showcaps=None,
        showbox=None,
        showfliers=None,
        **kwargs,
    ):
        # Missing arguments default to rcParams.
        if whis is None:
            whis = mat.rcParams["boxplot.whiskers"]
        if bootstrap is None:
            bootstrap = mat.rcParams["boxplot.bootstrap"]

        if notch is None:
            notch = mat.rcParams["boxplot.notch"]
        if vert is None:
            vert = mat.rcParams["boxplot.vertical"]
        if patch_artist is None:
            patch_artist = mat.rcParams["boxplot.patchartist"]
        if meanline is None:
            meanline = mat.rcParams["boxplot.meanline"]
        if showmeans is None:
            showmeans = mat.rcParams["boxplot.showmeans"]
        if showcaps is None:
            showcaps = mat.rcParams["boxplot.showcaps"]
        if showbox is None:
            showbox = mat.rcParams["boxplot.showbox"]
        if showfliers is None:
            showfliers = mat.rcParams["boxplot.showfliers"]

        return dict(
            whis=whis,
            bootstrap=bootstrap,
            notch=notch,
            vert=vert,
            patch_artist=patch_artist,
            meanline=meanline,
            showmeans=showmeans,
            showcaps=showcaps,
            showbox=showbox,
            showfliers=showfliers,
        )


class PandasOnSparkHistPlot(PandasHistPlot, HistogramPlotBase):
    _kind = "hist"

    def _args_adjust(self):
        if is_list_like(self.bottom):
            self.bottom = np.array(self.bottom)

    @final
    def _ensure_frame(self, data):
        if isinstance(data, Series):
            label = self.label
            if label is None and data.name is None:
                label = ""
            if label is None:
                data = data.to_frame()
            else:
                data = data.to_frame(name=label)
        return data

    def _calculate_bins(self, data, bins):
        return bins

    def _compute_plot_data(self):
        self.data, self.bins = HistogramPlotBase.prepare_hist_data(self.data, self.bins)

    def _make_plot_keywords(self, kwds, y):
        """merge BoxPlot/KdePlot properties to passed kwds"""
        # y is required for KdePlot
        kwds["bottom"] = self.bottom
        kwds["bins"] = self.bins
        return kwds

    def _make_plot(self, fig: Figure):
        # TODO: this logic is similar to KdePlot. Might have to deduplicate it.
        # 'num_colors' requires to calculate `shape` which has to count all.
        # Use 1 for now to save the computation.
        colors = self._get_colors(num_colors=1)
        stacking_id = self._get_stacking_id()
        output_series = HistogramPlotBase.compute_hist(self.data, self.bins)

        for (i, label), y in zip(enumerate(self.data._internal.column_labels), output_series):
            ax = self._get_ax(i)

            kwds = self.kwds.copy()

            label = pprint_thing(label if len(label) > 1 else label[0])
            # `if hasattr(...)` makes plotting compatible with pandas < 1.3,
            # see pandas-dev/pandas#40078.
            label = (
                self._mark_right_label(label, index=i)
                if hasattr(self, "_mark_right_label")
                else label
            )
            kwds["label"] = label

            style, kwds = self._apply_style_colors(colors, kwds, i, label)
            if style is not None:
                kwds["style"] = style

            kwds = self._make_plot_keywords(kwds, y)
            artists = self._plot(ax, y, column_num=i, stacking_id=stacking_id, **kwds)
            # `if hasattr(...)` makes plotting compatible with pandas < 1.3,
            # see pandas-dev/pandas#40078.
            self._append_legend_handles_labels(artists[0], label) if hasattr(
                self, "_append_legend_handles_labels"
            ) else self._add_legend_handle(artists[0], label, index=i)

    @classmethod
    def _plot(cls, ax, y, style=None, bins=None, bottom=0, column_num=0, stacking_id=None, **kwds):
        if column_num == 0:
            cls._initialize_stacker(ax, stacking_id, len(bins) - 1)

        base = np.zeros(len(bins) - 1)
        bottom = bottom + cls._get_stacked_values(ax, stacking_id, base, kwds["label"])

        # Since the counts were computed already, we use them as weights and just generate
        # one entry for each bin
        n, bins, patches = ax.hist(bins[:-1], bins=bins, bottom=bottom, weights=y, **kwds)

        cls._update_stacker(ax, stacking_id, n)
        return patches


class PandasOnSparkPiePlot(PandasPiePlot, TopNPlotBase):
    _kind = "pie"

    def __init__(self, data, **kwargs):
        super().__init__(self.get_top_n(data), **kwargs)

    def _make_plot(self, fig: Figure):
        self.set_result_text(self._get_ax(0))
        super()._make_plot(fig)


class PandasOnSparkAreaPlot(PandasAreaPlot, SampledPlotBase):
    _kind = "area"

    def __init__(self, data, **kwargs):
        super().__init__(self.get_sampled(data), **kwargs)

    def _make_plot(self, fig: Figure):
        self.set_result_text(self._get_ax(0))
        super()._make_plot(fig)


class PandasOnSparkLinePlot(PandasLinePlot, SampledPlotBase):
    _kind = "line"

    def __init__(self, data, **kwargs):
        super().__init__(self.get_sampled(data), **kwargs)

    def _make_plot(self, fig: Figure):
        self.set_result_text(self._get_ax(0))
        super()._make_plot(fig)


class PandasOnSparkBarhPlot(PandasBarhPlot, TopNPlotBase):
    _kind = "barh"

    def __init__(self, data, **kwargs):
        super().__init__(self.get_top_n(data), **kwargs)

    def _make_plot(self, fig: Figure):
        self.set_result_text(self._get_ax(0))
        super()._make_plot(fig)


class PandasOnSparkScatterPlot(PandasScatterPlot, TopNPlotBase):
    _kind = "scatter"

    def __init__(self, data, x, y, **kwargs):
        super().__init__(self.get_top_n(data), x, y, **kwargs)

    def _make_plot(self, fig: Figure):
        self.set_result_text(self._get_ax(0))
        super()._make_plot(fig)


class PandasOnSparkKdePlot(PandasKdePlot, KdePlotBase):
    _kind = "kde"

    def _compute_plot_data(self):
        self.data = KdePlotBase.prepare_kde_data(self.data)

    def _make_plot_keywords(self, kwds, y):
        kwds["bw_method"] = self.bw_method
        kwds["ind"] = type(self)._get_ind(y, ind=self.ind)
        return kwds

    def _make_plot(self, fig: Figure):
        # 'num_colors' requires to calculate `shape` which has to count all.
        # Use 1 for now to save the computation.
        colors = self._get_colors(num_colors=1)
        stacking_id = self._get_stacking_id()

        sdf = self.data._internal.spark_frame

        for i, label in enumerate(self.data._internal.column_labels):
            # 'y' is a Spark DataFrame that selects one column.
            y = sdf.select(self.data._internal.spark_column_for(label))
            ax = self._get_ax(i)

            kwds = self.kwds.copy()

            label = pprint_thing(label if len(label) > 1 else label[0])
            # `if hasattr(...)` makes plotting compatible with pandas < 1.3,
            # see pandas-dev/pandas#40078.
            label = (
                self._mark_right_label(label, index=i)
                if hasattr(self, "_mark_right_label")
                else label
            )
            kwds["label"] = label

            style, kwds = self._apply_style_colors(colors, kwds, i, label)
            if style is not None:
                kwds["style"] = style

            kwds = self._make_plot_keywords(kwds, y)
            artists = self._plot(ax, y, column_num=i, stacking_id=stacking_id, **kwds)
            # `if hasattr(...)` makes plotting compatible with pandas < 1.3,
            # see pandas-dev/pandas#40078.
            self._append_legend_handles_labels(artists[0], label) if hasattr(
                self, "_append_legend_handles_labels"
            ) else self._add_legend_handle(artists[0], label, index=i)

    @staticmethod
    def _get_ind(y, ind):
        return KdePlotBase.get_ind(y, ind)

    @classmethod
    def _plot(
        cls, ax, y, style=None, bw_method=None, ind=None, column_num=None, stacking_id=None, **kwds
    ):
        y = KdePlotBase.compute_kde(y, bw_method=bw_method, ind=ind)
        lines = PandasMPLPlot._plot(ax, ind, y, style=style, **kwds)
        return lines


_klasses = [
    PandasOnSparkHistPlot,
    PandasOnSparkBarPlot,
    PandasOnSparkBoxPlot,
    PandasOnSparkPiePlot,
    PandasOnSparkAreaPlot,
    PandasOnSparkLinePlot,
    PandasOnSparkBarhPlot,
    PandasOnSparkScatterPlot,
    PandasOnSparkKdePlot,
]
_plot_klass = {getattr(klass, "_kind"): klass for klass in _klasses}
_common_kinds = {"area", "bar", "barh", "box", "hist", "kde", "line", "pie"}
_series_kinds = _common_kinds.union(set())
_dataframe_kinds = _common_kinds.union({"scatter", "hexbin"})
_pandas_on_spark_all_kinds = _common_kinds.union(_series_kinds).union(_dataframe_kinds)


def plot_pandas_on_spark(data, kind, **kwargs):
    if kind not in _pandas_on_spark_all_kinds:
        raise ValueError("{} is not a valid plot kind".format(kind))

    from pyspark.pandas import DataFrame, Series

    if isinstance(data, Series):
        if kind not in _series_kinds:
            return unsupported_function(class_name="pd.Series", method_name=kind)()
        return plot_series(data=data, kind=kind, **kwargs)
    elif isinstance(data, DataFrame):
        if kind not in _dataframe_kinds:
            return unsupported_function(class_name="pd.DataFrame", method_name=kind)()
        return plot_frame(data=data, kind=kind, **kwargs)


def plot_series(
    data,
    kind="line",
    ax=None,  # Series unique
    figsize=None,
    use_index=True,
    title=None,
    grid=None,
    legend=False,
    style=None,
    logx=False,
    logy=False,
    loglog=False,
    xticks=None,
    yticks=None,
    xlim=None,
    ylim=None,
    rot=None,
    fontsize=None,
    colormap=None,
    table=False,
    yerr=None,
    xerr=None,
    label=None,
    secondary_y=False,  # Series unique
    **kwds,
):
    """
    Make plots of Series using matplotlib / pylab.

    Each plot kind has a corresponding method on the
    ``Series.plot`` accessor:
    ``s.plot(kind='line')`` is equivalent to
    ``s.plot.line()``.

    Parameters
    ----------
    data : Series

    kind : str
        - 'line' : line plot (default)
        - 'bar' : vertical bar plot
        - 'barh' : horizontal bar plot
        - 'hist' : histogram
        - 'box' : boxplot
        - 'kde' : Kernel Density Estimation plot
        - 'density' : same as 'kde'
        - 'area' : area plot
        - 'pie' : pie plot

    ax : matplotlib axes object
        If not passed, uses gca()
    figsize : a tuple (width, height) in inches
    use_index : boolean, default True
        Use index as ticks for x axis
    title : string or list
        Title to use for the plot. If a string is passed, print the string at
        the top of the figure. If a list is passed and `subplots` is True,
        print each item in the list above the corresponding subplot.
    grid : boolean, default None (matlab style default)
        Axis grid lines
    legend : False/True/'reverse'
        Place legend on axis subplots
    style : list or dict
        matplotlib line style per column
    logx : boolean, default False
        Use log scaling on x axis
    logy : boolean, default False
        Use log scaling on y axis
    loglog : boolean, default False
        Use log scaling on both x and y axes
    xticks : sequence
        Values to use for the xticks
    yticks : sequence
        Values to use for the yticks
    xlim : 2-tuple/list
    ylim : 2-tuple/list
    rot : int, default None
        Rotation for ticks (xticks for vertical, yticks for horizontal plots)
    fontsize : int, default None
        Font size for xticks and yticks
    colormap : str or matplotlib colormap object, default None
        Colormap to select colors from. If string, load colormap with that name
        from matplotlib.
    colorbar : boolean, optional
        If True, plot colorbar (only relevant for 'scatter' and 'hexbin' plots)
    position : float
        Specify relative alignments for bar plot layout.
        From 0 (left/bottom-end) to 1 (right/top-end). Default is 0.5 (center)
    table : boolean, Series or DataFrame, default False
        If True, draw a table using the data in the DataFrame and the data will
        be transposed to meet matplotlib's default layout.
        If a Series or DataFrame is passed, use passed data to draw a table.
    yerr : DataFrame, Series, array-like, dict and str
        See :ref:`Plotting with Error Bars <visualization.errorbars>` for
        detail.
    xerr : same types as yerr.
    label : label argument to provide to plot
    secondary_y : boolean or sequence of ints, default False
        If True then y-axis will be on the right
    mark_right : boolean, default True
        When using a secondary_y axis, automatically mark the column
        labels with "(right)" in the legend
    **kwds : keywords
        Options to pass to matplotlib plotting method

    Returns
    -------
    axes : :class:`matplotlib.axes.Axes` or numpy.ndarray of them

    Notes
    -----

    - See matplotlib documentation online for more on this subject
    - If `kind` = 'bar' or 'barh', you can specify relative alignments
      for bar plot layout by `position` keyword.
      From 0 (left/bottom-end) to 1 (right/top-end). Default is 0.5 (center)
    """

    # function copied from pandas.plotting._core
    # so it calls modified _plot below

    import matplotlib.pyplot as plt

    if ax is None and len(plt.get_fignums()) > 0:
        with plt.rc_context():
            ax = plt.gca()
        ax = PandasMPLPlot._get_ax_layer(ax)
    return _plot(
        data,
        kind=kind,
        ax=ax,
        figsize=figsize,
        use_index=use_index,
        title=title,
        grid=grid,
        legend=legend,
        style=style,
        logx=logx,
        logy=logy,
        loglog=loglog,
        xticks=xticks,
        yticks=yticks,
        xlim=xlim,
        ylim=ylim,
        rot=rot,
        fontsize=fontsize,
        colormap=colormap,
        table=table,
        yerr=yerr,
        xerr=xerr,
        label=label,
        secondary_y=secondary_y,
        **kwds,
    )


def plot_frame(
    data,
    x=None,
    y=None,
    kind="line",
    ax=None,
    subplots=False,
    sharex=None,
    sharey=False,
    layout=None,
    figsize=None,
    use_index=True,
    title=None,
    grid=None,
    legend=True,
    style=None,
    logx=False,
    logy=False,
    loglog=False,
    xticks=None,
    yticks=None,
    xlim=None,
    ylim=None,
    rot=None,
    fontsize=None,
    colormap=None,
    table=False,
    yerr=None,
    xerr=None,
    secondary_y=False,
    **kwds,
):
    """
    Make plots of DataFrames using matplotlib / pylab.

    Each plot kind has a corresponding method on the
    ``DataFrame.plot`` accessor:
    ``psdf.plot(kind='line')`` is equivalent to
    ``psdf.plot.line()``.

    Parameters
    ----------
    data : DataFrame

    kind : str
        - 'line' : line plot (default)
        - 'bar' : vertical bar plot
        - 'barh' : horizontal bar plot
        - 'hist' : histogram
        - 'box' : boxplot
        - 'kde' : Kernel Density Estimation plot
        - 'density' : same as 'kde'
        - 'area' : area plot
        - 'pie' : pie plot
        - 'scatter' : scatter plot
    ax : matplotlib axes object
        If not passed, uses gca()
    x : label or position, default None
    y : label, position or list of label, positions, default None
        Allows plotting of one column versus another.
    figsize : a tuple (width, height) in inches
    use_index : boolean, default True
        Use index as ticks for x axis
    title : string or list
        Title to use for the plot. If a string is passed, print the string at
        the top of the figure. If a list is passed and `subplots` is True,
        print each item in the list above the corresponding subplot.
    grid : boolean, default None (matlab style default)
        Axis grid lines
    legend : False/True/'reverse'
        Place legend on axis subplots
    style : list or dict
        matplotlib line style per column
    logx : boolean, default False
        Use log scaling on x axis
    logy : boolean, default False
        Use log scaling on y axis
    loglog : boolean, default False
        Use log scaling on both x and y axes
    xticks : sequence
        Values to use for the xticks
    yticks : sequence
        Values to use for the yticks
    xlim : 2-tuple/list
    ylim : 2-tuple/list
    sharex: bool or None, default is None
        Whether to share x axis or not.
    sharey: bool, default is False
        Whether to share y axis or not.
    rot : int, default None
        Rotation for ticks (xticks for vertical, yticks for horizontal plots)
    fontsize : int, default None
        Font size for xticks and yticks
    colormap : str or matplotlib colormap object, default None
        Colormap to select colors from. If string, load colormap with that name
        from matplotlib.
    colorbar : boolean, optional
        If True, plot colorbar (only relevant for 'scatter' and 'hexbin' plots)
    position : float
        Specify relative alignments for bar plot layout.
        From 0 (left/bottom-end) to 1 (right/top-end). Default is 0.5 (center)
    table : boolean, Series or DataFrame, default False
        If True, draw a table using the data in the DataFrame and the data will
        be transposed to meet matplotlib's default layout.
        If a Series or DataFrame is passed, use passed data to draw a table.
    yerr : DataFrame, Series, array-like, dict and str
        See :ref:`Plotting with Error Bars <visualization.errorbars>` for
        detail.
    xerr : same types as yerr.
    label : label argument to provide to plot
    secondary_y : boolean or sequence of ints, default False
        If True then y-axis will be on the right
    mark_right : boolean, default True
        When using a secondary_y axis, automatically mark the column
        labels with "(right)" in the legend
    **kwds : keywords
        Options to pass to matplotlib plotting method

    Returns
    -------
    axes : :class:`matplotlib.axes.Axes` or numpy.ndarray of them

    Notes
    -----

    - See matplotlib documentation online for more on this subject
    - If `kind` = 'bar' or 'barh', you can specify relative alignments
      for bar plot layout by `position` keyword.
      From 0 (left/bottom-end) to 1 (right/top-end). Default is 0.5 (center)
    """
    return _plot(
        data,
        kind=kind,
        x=x,
        y=y,
        ax=ax,
        figsize=figsize,
        use_index=use_index,
        title=title,
        grid=grid,
        legend=legend,
        subplots=subplots,
        style=style,
        logx=logx,
        logy=logy,
        loglog=loglog,
        xticks=xticks,
        yticks=yticks,
        xlim=xlim,
        ylim=ylim,
        rot=rot,
        fontsize=fontsize,
        colormap=colormap,
        table=table,
        yerr=yerr,
        xerr=xerr,
        sharex=sharex,
        sharey=sharey,
        secondary_y=secondary_y,
        layout=layout,
        **kwds,
    )


def _plot(data, x=None, y=None, subplots=False, ax=None, kind="line", **kwds):
    from pyspark.pandas import DataFrame

    # function copied from pandas.plotting._core
    # and adapted to handle pandas-on-Spark DataFrame and Series

    kind = kind.lower().strip()
    kind = {"density": "kde"}.get(kind, kind)
    if kind in _all_kinds:
        klass = _plot_klass[kind]
    else:
        raise ValueError("%r is not a valid plot kind" % kind)

    # scatter and hexbin are inherited from PlanePlot which require x and y
    if kind in ("scatter", "hexbin"):
        plot_obj = klass(data, x, y, subplots=subplots, ax=ax, kind=kind, **kwds)
    else:
        # check data type and do preprocess before applying plot
        if isinstance(data, DataFrame):
            if x is not None:
                data = data.set_index(x)
            # TODO: check if value of y is plottable
            if y is not None:
                data = data[y]

        plot_obj = klass(data, subplots=subplots, ax=ax, kind=kind, **kwds)
    plot_obj.generate()
    plot_obj.draw()
    return plot_obj.result
