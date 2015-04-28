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


var timelineMarginLeft = 50;
var distributionMinX = 0;
var distributionMaxX = 0;
var binCount = 10;

// An invisible div to show details of a point in the graph
var graphTooltip = d3.select("body").append("div")
    .attr("class", "label")
    .style("display", "inline-block")
    .style("position", "absolute")
    .style("z-index", "10")
    .style("visibility", "hidden")
    .text("");

// Show "text" at location (x, y)
function showGraphTooltip(text, x, y) {
    var left = x;
    var top = y;
    graphTooltip.style("visibility", "visible")
        .style("top", top + "px")
        .style("left", left + "px")
        .text(text);
}

// Hide "graphTooltip"
function hideGraphTooltip() {
    graphTooltip.style("visibility", "hidden");
}

function showBootstrapTooltip(node, text) {
    console.log(text);
    $(node).tooltip({title: text, trigger: "manual", 'container': 'body'});
    $(node).tooltip("show");
    console.log($(node));
}

function hideBootstrapTooltip(node) {
    $(node).tooltip("destroy");
}

/**
 * @param id the `id` used in the html `div` tag
 * @param data the data for the timeline graph
 * @param minY the min value of Y axis
 * @param maxY the max value of Y axis
 * @param unitY the unit of Y axis
 */
function drawTimeline(id, data, minX, maxX, minY, maxY, unitY) {
    d3.select(d3.select(id).node().parentNode).style("padding", "8px 0 8px 8px").style("border-right", "0px solid white");
    var margin = {top: 20, right: 27, bottom: 30, left: timelineMarginLeft};
    var width = 500 - margin.left - margin.right;
    var height = 150 - margin.top - margin.bottom;

    var x = d3.scale.linear().domain([minX, maxX]).range([0, width]);
    var y = d3.scale.linear().domain([minY, maxY]).range([height, 0]);

    var xAxis = d3.svg.axis().scale(x).orient("bottom").tickFormat(function(d) { return timeFormat[d]; });
    var formatYValue = d3.format(",.2f");
    var yAxis = d3.svg.axis().scale(y).orient("left").ticks(5).tickFormat(formatYValue);

    var line = d3.svg.line()
        .x(function(d) { return x(d.x); })
        .y(function(d) { return y(d.y); });

    var svg = d3.select(id).append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    // Only show the first and last time in the graph
    xAxis.tickValues(x.domain());

    svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis)

    svg.append("g")
        .attr("class", "y axis")
        .call(yAxis)
        .append("text")
            .attr("transform", "translate(0," + (-3) + ")")
            .text(unitY);

    svg.append("path")
        .datum(data)
        .attr("class", "line")
        .attr("d", line);

    // Add points to the line. However, we make it invisible at first. But when the user moves mouse
    // over a point, it will be displayed with its detail.
    svg.selectAll(".point")
        .data(data)
        .enter().append("circle")
                    .attr("stroke", "white") // white and opacity = 0 make it invisible
                    .attr("fill", "white")
                    .attr("opacity", "0")
                    .attr("cx", function(d) { return x(d.x); })
                    .attr("cy", function(d) { return y(d.y); })
                    .attr("r", function(d) { return 3; })
                    .on('mouseover', function(d) {
                        var tip = formatYValue(d.y) + " " + unitY + " at " + timeFormat[d.x];
                        showBootstrapTooltip(d3.select(this).node(), tip);
                        //showGraphTooltip(tip, d3.event.pageX + 5, d3.event.pageY - 25);
                        // show the point
                        d3.select(this)
                            .attr("stroke", "steelblue")
                            .attr("fill", "steelblue")
                            .attr("opacity", "1");
                    })
                    .on('mouseout',  function() {
                        hideBootstrapTooltip(d3.select(this).node());
                        //hideGraphTooltip();
                        // hide the point
                        d3.select(this)
                            .attr("stroke", "white")
                            .attr("fill", "white")
                            .attr("opacity", "0");
                    });
}

/**
 * @param id the `id` used in the html `div` tag
 * @param data the data for the distribution graph
 * @param minY the min value of Y axis
 * @param maxY the max value of Y axis
 * @param unitY the unit of Y axis
 */
function drawDistribution(id, values, minY, maxY, unitY) {
    d3.select(d3.select(id).node().parentNode).style("padding", "8px 8px 8px 0").style("border-left", "0px solid white");
    var margin = {top: 20, right: 30, bottom: 30, left: 10};
    var width = 300 - margin.left - margin.right;
    var height = 150 - margin.top - margin.bottom;

    var formatBinValue = d3.format(",.2f");

    var y = d3.scale.linear().domain([minY, maxY]).range([height, 0]);
    var data = d3.layout.histogram().range([minY, maxY]).bins(binCount)(values);

    var x = d3.scale.linear()
        .domain([distributionMinX, distributionMaxX])
        .range([0, width]);

    var xAxis = d3.svg.axis().scale(x).orient("top").ticks(5);
    var yAxis = d3.svg.axis().scale(y).orient("left").ticks(0).tickFormat(function(d) { return ""; });

    var svg = d3.select(id).append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + 0 + ")")
        .call(xAxis)

    svg.append("g")
        .attr("class", "y axis")
        .call(yAxis)

    var bar = svg.selectAll(".bar")
        .data(data)
      .enter().append("g")
                  .attr("transform", function(d) { return "translate(0," + (y(d.x) - height + y(d.dx))  + ")";})
                  .attr("class", "bar").append("rect")
                  .attr("width", function(d) { return x(d.y); })
                  .attr("height", function(d) { return height - y(d.dx); })
                  .on('mouseover', function(d) {
                      var tip = d.y + " between " + formatBinValue(d.x) + " and " + formatBinValue(d.x + d.dx) + " " + unitY;
                      showBootstrapTooltip(d3.select(this).node(), tip);

                      // Calculate the location for tip
                      var scrollTop  = document.documentElement.scrollTop || document.body.scrollTop;
                      var scrollLeft = document.documentElement.scrollLeft || document.body.scrollLeft;
                      var target = d3.event.target;
                      var matrix = target.getScreenCTM();
                      var targetBBox = target.getBBox();
                      var point = svg.node().ownerSVGElement.createSVGPoint();
                      point.x = targetBBox.x;
                      point.y = targetBBox.y;
                      var bbox = point.matrixTransform(matrix);
                      var tipX = bbox.x + scrollLeft;
                      var tipY = bbox.y + scrollTop + 15;

                      //showGraphTooltip(tip, tipX, tipY);
                  })
                  .on('mouseout',  function() {
                      hideBootstrapTooltip(d3.select(this).node());
                      //hideGraphTooltip();
                  });
}

function prepareTimeline(minY, maxY) {
    var formatYValue = d3.format(",.2f");
    var numOfChars = formatYValue(maxY).length;
    var maxPx = numOfChars * 8 + 10;
    // Make sure we have enough space to show the ticks in the y axis of timeline
    timelineMarginLeft = maxPx > timelineMarginLeft? maxPx : timelineMarginLeft;
}

function prepareDistribution(values, minY, maxY) {
    var data = d3.layout.histogram().range([minY, maxY]).bins(binCount)(values);
    var maxBarSize = d3.max(data, function(d) { return d.y; });
    distributionMaxX = maxBarSize > distributionMaxX? maxBarSize : distributionMaxX;
}

function getUrlParameter(sParam)
{
    var sPageURL = window.location.search.substring(1);
    var sURLVariables = sPageURL.split('&');
    for (var i = 0; i < sURLVariables.length; i++)
    {
        var sParameterName = sURLVariables[i].split('=');
        if (sParameterName[0] == sParam)
        {
            return sParameterName[1];
        }
    }
}

$(function() {
    if (getUrlParameter("show-receivers-detail") == "true") {
        $('#inputs-table').toggle('collapsed');
        $('#triangle').html('&#9660;');
    }
});
