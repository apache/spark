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

export { drawAreaStack };
import { getMaxMarginLeftForTimeline, hideBootstrapTooltip, showBootstrapTooltip } from './streaming-page.js';
/* global, d3 */
// pre-define some colors for legends.
var colorPool = ["#F8C471", "#F39C12", "#B9770E", "#73C6B6", "#16A085", "#117A65", "#B2BABB", "#7F8C8D", "#616A6B"];

const unitLabelYOffset = -10;

/* eslint-disable no-undef */
function drawAreaStack(id, labels, values) {
  d3.select(d3.select(id).node().parentNode)
    .style("padding", "8px 0 8px 8px")
    .style("border-right", "0px solid white");

  // Setup svg using Bostock's margin convention
  var margin = {top: 20, right: 40, bottom: 30, left: getMaxMarginLeftForTimeline()};
  var width = 850 - margin.left - margin.right;
  var height = 300 - margin.top - margin.bottom;

  var svg = d3.select(id)
    .append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  var data = values.flatMap(function(d) {
    return Object.keys(d).filter(k => k !== "x").map(function(key) {
      return {x: d.x, label: key, duration: +d[key]};
    });
  });

  var dataset = d3.stack()
    .keys(labels)
    .value(([, group], label) => group.get(label).duration)(
      d3.index(data, d => d.x, d => d.label));

  // Set x, y and colors
  var x = d3.scaleBand()
    .domain(dataset[0].map(d => d.data[0]))
    .range([10, width-10], 0.02);

  var y = d3.scaleLinear()
    .domain([0, d3.max(dataset, layer => d3.max(layer, d => d[1]))])
    .range([height, 0]);

  var colors = colorPool.slice(0, labels.length);

  // Define and draw axes
  var yAxis = d3.axisLeft(y).ticks(7).tickFormat( function(d) { return d } );

  var xAxis = d3.axisBottom(x).tickFormat(function(d) { return d });

  // Only show the first and last time in the graph
  var xline = [];
  xline.push(x.domain()[0]);
  xline.push(x.domain()[x.domain().length - 1]);
  xAxis.tickValues(xline);

  svg.append("g")
    .attr("class", "y axis")
    .call(yAxis)
    .append("text")
    .attr("transform", "translate(0," + unitLabelYOffset + ")")
    .text("ms");

  svg.append("g")
    .attr("class", "x axis")
    .attr("transform", "translate(0," + height + ")")
    .call(xAxis);

  // Create groups for each series, rects for each segment
  var groups = svg.selectAll("g.cost")
    .data(dataset)
    .enter().append("g")
    .attr("class", "cost")
    .style("fill", function(d, i) { return colors[i]; });

  groups.selectAll("rect")
    .data(function(d) { return d; })
    .enter()
    .append("rect")
    .attr("x", d => x(d.data[0]))
    .attr("y", d => y(d[1]))
    .attr("height", d => y(d[0]) - y(d[1]))
    .attr("width", x.bandwidth())
    .on('mouseover', function(event, d) {
      var tip = '';
      d.data[1].forEach(function (k) {
        tip += k.label + ': ' + k.duration + '   ';
      });
      tip += " at " + d.data[0];
      showBootstrapTooltip(d3.select(this), tip);
    })
    .on('mouseout',  function() {
      hideBootstrapTooltip(d3.select(this));
    })
    .on("mousemove", (event, d) => {
      var xPosition = d3.pointer(event)[0] - 15;
      var yPosition = d3.pointer(event)[1] - 25;
      tooltip.attr("transform", "translate(" + xPosition + "," + yPosition + ")");
      tooltip.select("text").text(d[1]);
    });

  // Draw legend
  var legend = svg.selectAll(".legend")
    .data(colors)
    .enter().append("g")
    .attr("class", "legend")
    .attr("transform", function(d, i) { return "translate(30," + i * 19 + ")"; });

  legend.append("rect")
    .attr("x", width - 20)
    .attr("width", 18)
    .attr("height", 18)
    .style("fill", function(d, i) {return colors.slice().reverse()[i];})
    .on('mouseover', function(event, d) {
      showBootstrapTooltip(d3.select(this), labels[labels.length - 1 - colors.indexOf(d)]);
    })
    .on('mouseout',  function() {
      hideBootstrapTooltip(d3.select(this));
    });

  // Prep the tooltip bits, initial display is hidden
  var tooltip = svg.append("g")
    .attr("class", "tooltip")
    .style("display", "none");

  tooltip.append("rect")
    .attr("width", 30)
    .attr("height", 20)
    .attr("fill", "white")
    .style("opacity", 0.5);

  tooltip.append("text")
    .attr("x", 15)
    .attr("dy", "1.2em")
    .style("text-anchor", "middle")
    .attr("font-size", "12px")
    .attr("font-weight", "bold");
}
/* eslint-enable no-undef */
