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

var stageVizIsRendered = false

/*
 * Render or remove the stage visualization on the UI.
 * This assumes that the visualization is stored in the "#viz-graph" element.
 */
function toggleStageViz() {
  $(".expand-visualization-arrow").toggleClass('arrow-closed');
  $(".expand-visualization-arrow").toggleClass('arrow-open');
  var shouldRender = $(".expand-visualization-arrow").hasClass("arrow-open");
  if (shouldRender) {
    // If the viz is already rendered, just show it
    if (stageVizIsRendered) {
      $("#viz-graph").show();
    } else {
      renderStageViz();
      stageVizIsRendered = true;
    }
  } else {
    // Instead of emptying the element once and for all, cache it for use
    // again later in case we want to expand the visualization again
    $("#viz-graph").hide();
  }
}

/*
 * Render a DAG that describes the RDDs for a given stage.
 *
 * Input: The content of a dot file, stored in the text of the "#viz-dot-file" element
 * Output: An SVG that visualizes the DAG, stored in the "#viz-graph" element
 *
 * This relies on a custom implementation of dagre-d3, which can be found under
 * http://github.com/andrewor14/dagre-d3/dist/dagre-d3.js. For more detail, please
 * track the changes in that project after it was forked.
 */
function renderStageViz() {

  // If there is not a dot file to render, report error
  if (d3.select("#viz-dot-file").empty()) {
    d3.select("#viz-graph")
      .append("div")
      .text("No visualization information available for this stage.");
    return;
  }

  // Parse the dot file and render it in an SVG
  var dot = d3.select("#viz-dot-file").text();
  var escaped_dot = dot
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, "\"");
  var g = graphlibDot.read(escaped_dot);
  var render = new dagreD3.render();
  var svg = d3.select("#viz-graph").append("svg");
  svg.call(render, g);

  // Set the appropriate SVG dimensions to ensure that all elements are displayed
  var svgMargin = 20;
  var boundingBox = svg.node().getBBox();
  svg.style("width", (boundingBox.width + svgMargin) + "px");
  svg.style("height", (boundingBox.height + svgMargin) + "px"); 

  // Add style to clusters, nodes and edges
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
      var labelText = cluster.attr("id").replace(/^cluster/, "").replace(/_.*$/, "");
      cluster.append("text")
        .attr("x", labelX)
        .attr("y", labelY)
        .attr("fill", "#AAAAAA")
        .attr("font-size", "11px")
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

/* Helper method to convert attributes to numeric values. */
function toFloat(f) {
  return parseFloat(f.replace(/px$/, ""))
}

