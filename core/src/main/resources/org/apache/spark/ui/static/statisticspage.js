/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var colorPool = ["#1f77b4",
    "#aec7e8",
    "#ff7f0e",
    "#ffbb78",
    "#2ca02c",
    "#98df8a",
    "#d62728",
    "#ff9896",
    "#9467bd",
    "#c5b0d5",
    "#8c564b",
    "#c49c94",
    "#e377c2",
    "#f7b6d2",
    "#7f7f7f",
    "#c7c7c7",
    "#bcbd22",
    "#dbdb8d",
    "#17becf",
    "#9edae5"];

var bisectDate = d3.bisector(function (d) {
    return +d.key;
}).left;

var formatYValue = d3.format(",.2f");

function formatStyle(name) {
    return 's' + name.replace(' ', '').replace('-', '').replace('%', '');
}

function getTimeWithServerZone(v, serverTimezoneOffset) {
    return v + new Date().getTimezoneOffset() * 60 * 1000 + serverTimezoneOffset * 60 * 1000
}

function addBrushArea(svg, line, lineId, x, y, xAxis, minX, maxX, width, height, updateLineFunc) {
    var brush = d3.svg.brush()
        .x(x).y(y)
        .extent([[0, 0], [width, height]])
        .on("brushend", updateChart);

    var idleTimeout
    function idled() { idleTimeout = null; }

    function updateChart() {
        var extent = brush.extent();

        if (brush.empty()) {
            if (!idleTimeout) return idleTimeout = setTimeout(idled, 350);
            x.domain([minX, maxX])
        } else {
            x.domain([extent[0][0].getTime(), extent[1][0].getTime()])
            line.select(".brush").call(brush.clear())
        }

        xAxis.transition().duration(1000).call(d3.svg.axis().scale(x).orient("bottom"))
        line
            .selectAll(lineId)
            .transition()
            .duration(1000)
            .attr("d", updateLineFunc)
    }

    svg.on("dblclick", function () {
        x.domain([minX, maxX])
        xAxis.transition().call(d3.svg.axis().scale(x).orient("bottom"))
        line
            .selectAll(lineId)
            .transition()
            .attr("d", updateLineFunc)
    });
    return brush;
}

function addToolTip(svg, context, x, nestY, height, unit, timezoneOffset, tooltipData) {
    var focus = svg.append("g")
        .style("display", "none");
    var tooltipLine = focus.append("line")
        .attr("class", "x")
        .style("stroke", "blue")
        .style("stroke-dasharray", "3,3")
        .style("opacity", 0.5)
        .attr("y1", height)
        .attr("y2", 0);
    context.select(".background")
        .attr("height", height)
        .on("mouseover.tooltip", function () {
            focus.style("display", null);
        })
        .on("mouseout.tooltip", function () {
            focus.style("display", "none");
            hideBootstrapTooltip(tooltipLine.node());
        })
        .on("mousemove.tooltip", mousemove);

    function mousemove() {
        var x0 = x.invert(d3.mouse(this)[0]).getTime();
        var i = bisectDate(nestY, x0, 1);
        var d0 = nestY[i - 1];
        var d1 = nestY[i];
        var d = x0 - d0.key > d1.key - x0 ? d1 : d0;
        tooltipLine
            .attr("transform",
                "translate(" + x(d.key) + "," +
                0 + ")");
        var stageIds = tooltipData.find(function(t) {
            return +d.key == getTimeWithServerZone(+t.x, timezoneOffset);
        });
        var stageIdStr = "";
        if (stageIds != undefined) stageIdStr = stageIds.id;
        var tip = "Time: " + d3.time.format("%X")(new Date(+d.key)) + ";\n " +
            "Associated stages: (" + stageIdStr + ");\n" +
            d.values.map(function (d) {
            var tip = d.name + ": " + formatYValue(d.y);
            if (unit == "%" || unit == "ms" || unit.includes("/")) {
                tip += unit;
            } else {
                tip += "(" + unit + ")";
            }
            return tip;
        }).sort().join("; ");
        hideBootstrapTooltip(tooltipLine.node());
        showBootstrapTooltip(tooltipLine.node(), tip);
    }
}

function addLegends(svg, line, lineId, res, height, legendMarginX, legendMarginY, legendSize, legendTextSize, legendCountInLine, colors) {
    var highlight = function (d) {
        line.selectAll(lineId).style("opacity", .1)
        line.select("." + formatStyle(d)).style("opacity", 1)
    }

    var noHighlight = function (d) {
        line.selectAll(lineId).style("opacity", 1)
    }

    svg.selectAll("myrect")
        .data(res)
        .enter()
        .append("rect")
        .attr("x", function (d, i) { return legendMarginX * 3 + (i % legendCountInLine) * (legendMarginX * 2 + legendSize); })
        .attr("y", function (d, i) { return height + legendMarginY * 3 + Math.floor((i / legendCountInLine)) * (legendSize + legendTextSize + legendMarginY); })
        .attr("width", legendSize)
        .attr("height", legendSize)
        .style("fill", function (d, i) { return colors(i); })
        .on("mouseover", highlight)
        .on("mouseleave", noHighlight);

    svg.selectAll("mylabels")
        .data(res)
        .enter()
        .append("text")
        .attr("x", function (d, i) { return legendMarginX * 3 + (i % legendCountInLine) * (legendMarginX * 2 + legendSize); })
        .attr("y", function (d, i) { return height + legendMarginY * 3 + legendSize + legendMarginY + Math.floor((i / legendCountInLine)) * (legendSize + legendTextSize + legendMarginY); })
        .style("fill", function (d, i) { return colors(i); })
        .text(function (d) { return d })
        .attr("font-size", "10px")
        .attr("text-anchor", "right")
        .style("alignment-baseline", "middle")
        .on("mouseover", highlight)
        .on("mouseleave", noHighlight);
}

function renderBrushMultiLineChart(data, id, unit, longLegend, timezoneOffset, tooltipData) {
    var nest = d3.nest()
        .key(function (d) { return d.name; })
        .rollup(function (v) {
            return v.map(function (e) {
                return { x: +getTimeWithServerZone(e.x, timezoneOffset), y: e.y };
            })
        })
        .entries(data);
    var nestY = d3.nest().key(function (d) { return +getTimeWithServerZone(d.x, timezoneOffset); })
        .entries(data);
    var res = nest.map(function (d) { return d.key });
    var resX = nestY.map(function (d) { return +d.key });
    var maxY = d3.max(nest, function (d) { return d3.max(d.values, function (d) { return +d.y; }) });
    var minY = d3.min(nest, function (d) { return d3.min(d.values, function (d) { return +d.y; }) });
    var maxX = d3.max(resX);
    var minX = d3.min(resX);
    var colors = d3.scale.ordinal()
        .domain(res)
        .range(colorPool);
    var legendSize = 20;
    var legendMarginY = 10;
    var legendMarginX = 10;
    if (longLegend) {
        legendMarginX = 30;
    }
    var legendTextSize = 10;

    var margin = { top: 10, right: 30, bottom: 30, left: 60 };
    var width = 1000 - margin.left - margin.right;
    var height = 400 - margin.top - margin.bottom;
    var legendCountInLine = Math.floor((width - legendMarginX * 3) / (legendSize + 2 * legendMarginX));

    var legendHeight = legendMarginY * 3 + (1 + res.length / legendCountInLine) * (legendSize + legendMarginY + legendTextSize);

    var svg = d3.select(id)
        .append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom + legendHeight)
        .append("g")
        .attr("transform",
            "translate(" + margin.left + "," + margin.top + ")");

    var x = d3.time.scale()
        .domain([minX, maxX])
        .range([0, width]);

    xAxis = svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(d3.svg.axis().scale(x).orient("bottom"));

    var yMargin = Math.floor((maxY - minY) / 20);
    if (minY > yMargin) {
        minY = minY - yMargin;
    }
    var y = d3.scale.linear()
        .domain([minY, maxY + yMargin])
        .range([height, 0]);

    yAxis = svg.append("g")
        .attr("class", "y axis")
        .call(d3.svg.axis().scale(y).orient("left").tickFormat(formatYValue))
        .append("text")
        .text(unit);

    var clip = svg.append("defs").append("svg:clipPath")
        .attr("id", "clip")
        .append("svg:rect")
        .attr("width", width)
        .attr("height", height + legendHeight)
        .attr("x", 0)
        .attr("y", 0);

    var line = svg.append('g')
        .attr("clip-path", "url(#clip)")

    line.selectAll("nest")
        .data(nest)
        .enter().append("path")
        .attr("class", function (d) { return "line " + formatStyle(d.key); })
        .attr("fill", "none")
        .attr("stroke",
            function (d, i) { return colors(i); })
        .attr("stroke-width", 1.5)
        .attr("d", function (d) {
            return d3.svg.line()
                .x(function (d) { return x(d.x); })
                .y(function (d) { return y(+d.y); })
                (d.values)
        });

    var context = line
        .append("g")
        .attr("class", "brush");
    var brush = addBrushArea(svg, line, ".line", x, y, xAxis, minX, maxX, width, height, function (d) {
        return d3.svg.line()
            .x(function (d) { return x(d.x); })
            .y(function (d) { return y(+d.y); })
            (d.values)
    });
    context.call(brush);

    addToolTip(svg, context, x, nestY, height, unit, timezoneOffset, tooltipData);

    addLegends(svg, line, ".line", res, height, legendMarginX, legendMarginY, legendSize, legendTextSize, legendCountInLine, colors);
}

function renderBrushStackAreaChart(data, id, unit, longLegend, timezoneOffset, tooltipData) {
    var nest = d3.nest()
        .key(function (d) { return d.name; })
        .rollup(function (v) {
            return v.map(function (e) {
                return { x: +getTimeWithServerZone(e.x, timezoneOffset), y: e.y };
            })
        })
        .entries(data);
    var res = nest.map(function (d) { return d.key });
    var nestY = d3.nest().key(function (d) { return +getTimeWithServerZone(d.x, timezoneOffset); })
        .entries(data);
    var resX = nestY.map(function (d) { return +d.key });
    var maxY = d3.max(nestY, function (d) { return d3.sum(d.values, function (d) { return +d.y; }) });
    var minY = d3.min(nest, function (d) { return d3.min(d.values, function (d) { return +d.y; }) });
    var maxX = d3.max(resX);
    var minX = d3.min(resX);
    var colors = d3.scale.ordinal()
        .domain(res)
        .range(colorPool);
    var legendSize = 20;
    var legendMarginY = 10;
    var legendMarginX = 10;
    if (longLegend) {
        legendMarginX = 30;
    }
    var legendTextSize = 10;

    var margin = { top: 10, right: 30, bottom: 30, left: 60 };
    var width = 1000 - margin.left - margin.right;
    var height = 400 - margin.top - margin.bottom;
    var legendCountInLine = Math.floor((width - legendMarginX * 3) / (legendSize + 2 * legendMarginX));

    var legendHeight = legendMarginY * 3 + (1 + res.length / legendCountInLine) * (legendSize + legendMarginY + legendTextSize);

    var svg = d3.select(id)
        .append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom + legendHeight)
        .append("g")
        .attr("transform",
            "translate(" + margin.left + "," + margin.top + ")");

    var stack = d3.layout.stack()
        .values(function (d) { return d.values; });
    var dataset = stack(nest);
    var x = d3.time.scale()
        .domain([minX, maxX])
        .range([0, width]);

    var yMargin = Math.floor((maxY - minY) / 20);
    if (minY > yMargin) {
        minY = minY - yMargin;
    }
    var y = d3.scale.linear()
        .domain([minY, maxY + yMargin])
        .range([height, 0]);

    xAxis = svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(d3.svg.axis().scale(x).orient("bottom"));
    yAxis = svg.append("g")
        .attr("class", "y axis")
        .call(d3.svg.axis().scale(y).orient("left").tickFormat(formatYValue))
        .append("text")
        .text(unit);
    var clip = svg.append("defs").append("svg:clipPath")
        .attr("id", "clip")
        .append("svg:rect")
        .attr("width", width)
        .attr("height", height + legendHeight)
        .attr("x", 0)
        .attr("y", 0);

    var areaChart = svg.append('g')
        .attr("clip-path", "url(#clip)")
    var area = d3.svg.area()
        .x(function (d) { return x(d.x); })
        .y0(function (d) { return y(d.y0); })
        .y1(function (d) { return y(d.y0 + d.y); });

    areaChart.selectAll("path")
        .data(dataset)
        .enter()
        .append("path")
        .attr("class", function (d) { return "myArea " + formatStyle(d.key); })
        .style("fill", function (d, i) { return colors(i); })
        .attr("d", function (d) { return area(d.values); })
        .append("title")
        .text(function (d) { return d.key; });

    var context = areaChart
        .append("g")
        .attr("class", "brush");
    var brush = addBrushArea(svg, areaChart, "path", x, y, xAxis, minX, maxX, width, height, function (d) {
        return area(d.values);
    });
    context.call(brush);

    addToolTip(svg, context, x, nestY, height, unit, timezoneOffset, tooltipData);

    addLegends(svg, areaChart, ".myArea", res, height, legendMarginX, legendMarginY, legendSize, legendTextSize, legendCountInLine, colors);
}
