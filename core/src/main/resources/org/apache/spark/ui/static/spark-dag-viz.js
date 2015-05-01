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

var VizDefaults = {
  stageVizOffset: 160
};

/*
 * Show or hide the RDD DAG visualization on the UI.
 * This assumes that the visualization is stored in the "#viz-graph" element.
 */
function toggleShowViz(forJob) {
  $(".expand-visualization-arrow").toggleClass('arrow-closed');
  $(".expand-visualization-arrow").toggleClass('arrow-open');
  var show = $(".expand-visualization-arrow").hasClass("arrow-open");
  if (show) {
    var shouldRender = d3.select("#viz-graph svg").empty();
    if (shouldRender) {
      renderViz();
      styleViz(forJob);
    }
    $("#viz-graph").show();
  } else {
    $("#viz-graph").hide();
  }
}

/*
 * Render a DAG visualization that describes RDDs in a stage or a job.
 *
 * Input: The contents of the relevant dot files, stored in the "#viz-dot-files" element
 * Output: An SVG that visualizes the DAG, stored in the "#viz-graph" element
 *
 * This relies on a custom implementation of dagre-d3, which can be found under
 * http://github.com/andrewor14/dagre-d3/dist/dagre-d3.js. For more detail, please
 * track the changes in that project after it was forked.
 */
function renderViz() {

  // If there is not a dot file to render, report error
  if (d3.select("#viz-dot-files").empty()) {
    d3.select("#viz-graph")
      .append("div")
      .text("No visualization information available for this stage.");
    return;
  }

  var svg = d3.select("#viz-graph").append("svg");

  // Each div in #viz-dot-files stores the content of one dot file
  // Each dot file is used to generate the visualization for a stage
  d3.selectAll("#viz-dot-files div").each(function(d, i) {
    var div = d3.select(this);
    var stageId = div.attr("name");
    var dot = div.text();
    var container = svg.append("g").attr("id", "graph_" + stageId);
    // Move the container so it doesn't overlap with the existing ones
    container.attr("transform", "translate(" + VizDefaults.stageVizOffset * i + ", 0)");
    renderDot(dot, container);
  });

  // Set the appropriate SVG dimensions to ensure that all elements are displayed
  var svgMargin = 20;
  var boundingBox = svg.node().getBBox();
  svg.style("width", (boundingBox.width + svgMargin) + "px");
  svg.style("height", (boundingBox.height + svgMargin) + "px"); 

  // Add labels to clusters
  d3.selectAll("svg g.cluster").each(function(cluster_data) {
    var cluster = d3.select(this);
    cluster.selectAll("rect").each(function(rect_data) {
      var rect = d3.select(this);
      // Shift the boxes up a little
      rect.attr("y", toFloat(rect.attr("y")) - 10);
      rect.attr("height", toFloat(rect.attr("height")) + 10);
      var labelX = toFloat(rect.attr("x")) + toFloat(rect.attr("width")) - 5;
      var labelY = toFloat(rect.attr("y")) + 15;
      var labelText = cluster.attr("name").replace(/^cluster_/, "");
      cluster.append("text")
        .attr("x", labelX)
        .attr("y", labelY)
        .attr("text-anchor", "end")
        .text(labelText);
    });
  });

  // We have shifted a few elements upwards, so we should fix the SVG views
  var startX = -svgMargin;
  var startY = -svgMargin;
  var endX = toFloat(svg.style("width")) + svgMargin;
  var endY = toFloat(svg.style("height")) + svgMargin;
  var newViewBox = startX + " " + startY + " " + endX + " " + endY;
  svg.attr("viewBox", newViewBox);
}

/*
 *
 */
function renderDot(dot, container) {
  var escaped_dot = dot
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, "\"");
  var g = graphlibDot.read(escaped_dot);
  var renderer = new dagreD3.render();
  renderer(container, g);
}

/*
 * Style the visualization we just rendered.
 * We apply a different style depending on whether this is a stage or a job.
 */
function styleViz(forJob) {
  if (forJob) {
    styleJobViz();
  } else {
    styleStageViz();
  }
}

function styleJobViz() {
  d3.selectAll("svg g.cluster rect")
    .style("fill", "none")
    .style("stroke", "#AADFFF")
    .style("stroke-width", "4px")
    .style("stroke-opacity", "0.5");
  d3.selectAll("svg g.node rect")
    .style("fill", "white")
    .style("stroke", "black")
    .style("stroke-width", "2px")
    .style("fill-opacity", "0.8")
    .style("stroke-opacity", "0.9");
  d3.selectAll("svg g.edgePath path")
    .style("stroke", "black")
    .style("stroke-width", "2px");
  d3.selectAll("svg g.cluster text")
    .attr("fill", "#AAAAAA")
    .attr("font-size", "11px")
}

function styleStageViz() {
  d3.selectAll("svg g.cluster rect")
    .style("fill", "none")
    .style("stroke", "#AADFFF")
    .style("stroke-width", "4px")
    .style("stroke-opacity", "0.5");
  d3.selectAll("svg g.node rect")
    .style("fill", "white")
    .style("stroke", "black")
    .style("stroke-width", "2px")
    .style("fill-opacity", "0.8")
    .style("stroke-opacity", "0.9");
  d3.selectAll("svg g.edgePath path")
    .style("stroke", "black")
    .style("stroke-width", "2px");
  d3.selectAll("svg g.cluster text")
    .attr("fill", "#AAAAAA")
    .attr("font-size", "11px")
}

/* Helper method to convert attributes to numeric values. */
function toFloat(f) {
  return parseFloat(f.replace(/px$/, ""))
}

