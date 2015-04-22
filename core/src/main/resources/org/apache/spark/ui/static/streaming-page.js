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


// An invisible div to show details of a point in the graph
var graphTooltip = d3.select("body").append("div")
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

/**
 * @param id the `id` used in the html `div` tag
 * @param data the data for the timeline graph
 * @param minY the min value of Y axis
 * @param maxY the max value of Y axis
 * @param unitY the unit of Y axis
 */
function drawTimeline(id, data, minX, maxX, minY, maxY, unitY) {
    var margin = {top: 20, right: 30, bottom: 30, left: 50};
    var width = 500 - margin.left - margin.right;
    var height = 150 - margin.top - margin.bottom;

    var x = d3.time.scale().domain([minX, maxX]).range([0, width]);
    var y = d3.scale.linear().domain([minY, maxY]).range([height, 0]);

    var timeFormat = d3.time.format("%H:%M:%S")
    var xAxis = d3.svg.axis().scale(x).orient("bottom").ticks(10)
                    .tickFormat(function(d) {
                        if (d.getTime() == minX || d.getTime() == maxX) {
                            return timeFormat(d);
                        } else {
                            return "x";
                        }
                    });
    var yAxis = d3.svg.axis().scale(y).orient("left").ticks(5);

    var line = d3.svg.line()
        .x(function(d) { return x(new Date(d.x)); })
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
                        var tip = d.y + " " + unitY + " at " + timeFormat(new Date(d.x));
                        showGraphTooltip(tip, d3.event.pageX + 5, d3.event.pageY - 25);
                        // show the point
                        d3.select(this)
                            .attr("stroke", "steelblue")
                            .attr("fill", "steelblue")
                            .attr("opacity", "1");
                    })
                    .on('mouseout',  function() {
                        hideGraphTooltip();
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
    var margin = {top: 20, right: 30, bottom: 30, left: 50};
    var width = 300 - margin.left - margin.right;
    var height = 150 - margin.top - margin.bottom;

    //var binCount = values.length > 100 ? 100 : values.length;
    var binCount = 10;
    var formatBinValue = d3.format(",.2f");

    var y = d3.scale.linear().domain([minY, maxY]).range([height, 0]);
    var data = d3.layout.histogram().range([minY, maxY]).bins(binCount)(values);

    var x = d3.scale.linear()
        .domain([0, d3.max(data, function(d) { return d.y; })])
        .range([0, width]);

    var xAxis = d3.svg.axis().scale(x).orient("bottom").ticks(5);
    var yAxis = d3.svg.axis().scale(y).orient("left").ticks(5);

    var svg = d3.select(id).append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

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

    var bar = svg.selectAll(".bar")
        .data(data)
      .enter().append("g")
                  .attr("transform", function(d) { return "translate(0," + (y(d.x) - height + y(d.dx))  + ")";})
                  .attr("class", "bar").append("rect")
                  .attr("width", function(d) { return x(d.y); })
                  .attr("height", function(d) { return height - y(d.dx); })
                  .on('mouseover', function(d) {
                      var tip = "[" + formatBinValue(d.x) + ", " + formatBinValue(d.x + d.dx) + "): " + d.y;

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
                      var tipX = bbox.x + scrollLeft + x(d.y) + 2;
                      var tipY = bbox.y + scrollTop - 6;

                      showGraphTooltip(tip, tipX, tipY);
                  })
                  .on('mouseout',  function() {
                      hideGraphTooltip();
                  });
}
