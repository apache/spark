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

function drawTimeline(id, data, unit) {
    var margin = {top: 20, right: 20, bottom: 70, left: 50};
    var width = 500 - margin.left - margin.right;
    var height = 250 - margin.top - margin.bottom;

    var x = d3.time.scale().range([0, width]);
    var y = d3.scale.linear().range([height, 0]);

    var xAxis = d3.svg.axis().scale(x).orient("bottom").tickFormat(d3.time.format("%H:%M:%S"));
    var yAxis = d3.svg.axis().scale(y).orient("left");

    var line = d3.svg.line()
        .x(function(d) { return x(new Date(d.x)); })
        .y(function(d) { return y(d.y); });

    var svg = d3.select(id).append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
                .append("g")
                .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    x.domain(d3.extent(data, function(d) { return d.x; }));
    y.domain(d3.extent(data, function(d) { return d.y; }));

    svg.append("g")
      .attr("class", "x axis")
      .attr("transform", "translate(0," + height + ")")
      .call(xAxis)
      .selectAll("text")
        .style("text-anchor", "end")
        .attr("dx", "-.8em")
        .attr("dy", ".15em")
        .attr("transform", "rotate(-65)");

    svg.append("g")
      .attr("class", "y axis")
      .call(yAxis)
    .append("text")
      .attr("x", 25)
      .attr("y", -5)
      .attr("dy", ".71em")
      .style("text-anchor", "end")
      .text(unit);

    svg.append("path")
      .datum(data)
      .attr("class", "line")
      .attr("d", line);
}

function drawDistribution(id, values, unit) {
    var margin = {top: 10, right: 30, bottom: 30, left: 50};
    var width = 500 - margin.left - margin.right;
    var height = 250 - margin.top - margin.bottom;

    var y = d3.scale.linear()
        .domain(d3.extent(values, function(d) { return d; }))
        .range([height, 0]);
    var data = d3.layout.histogram().bins(y.ticks(10))(values);
    console.log(values)
    console.log(data)
    var barHeight = height / data.length

    var x = d3.scale.linear()
        .domain([0, d3.max(data, function(d) { return d.y; })])
        .range([0, width]);

    var xAxis = d3.svg.axis().scale(x).orient("bottom");
    var yAxis = d3.svg.axis().scale(y).orient("left");

    var svg = d3.select(id).append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
      .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var bar = svg.selectAll(".bar")
        .data(data)
      .enter().append("g")
        .attr("class", "bar")

    bar.append("rect")
        .attr("x", 1)
        .attr("y", function(d) { return y(d.x) - margin.bottom + margin.top; })
        .attr("width", function(d) { return x(d.y); })
        .attr("height", function(d) { return (height - margin.bottom) / data.length; });

    bar.append("text")
        .attr("dy", ".75em")
        .attr("x", function(d) { return x(d.y) + 10; })
        .attr("y", function(d) { return y(d.x) + 16 - margin.bottom; })
        .attr("text-anchor", "middle")
        .text(function(d) { return d.y; });

    svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis);

    svg.append("g")
        .attr("class", "y axis")
        .call(yAxis);
}