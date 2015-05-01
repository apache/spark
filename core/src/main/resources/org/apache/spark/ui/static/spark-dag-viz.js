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

var VizConstants = {
  stageSep: 50,
  rddScopeColor: "#AADFFF",
  rddDotColor: "#444444",
  rddColor: "#444444",
  stageScopeColor: "#FFDDEE",
  scopeLabelColor: "#888888",
  edgeColor: "#444444",
  edgeWidth: "1.5px",
  svgMarginX: 0,
  svgMarginY: 20
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
      renderViz(forJob);
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
function renderViz(forJob) {

  // If there is not a dot file to render, report error
  if (d3.select("#viz-dot-files").empty()) {
    d3.select("#viz-graph")
      .append("div")
      .text("No visualization information available for this stage.");
    return;
  }

  var svg = d3.select("#viz-graph").append("svg");

  // On the job page, the visualization for each stage will rendered separately
  // Thus, we will need to render the edges that cross stages later on our own
  var crossStageEdges = [];

  // Each div child in #viz-dot-files stores the content of one dot file
  // Each dot file is used to generate the visualization for a stage
  d3.selectAll("#viz-dot-files > div").each(function(d, i) {
    var div = d3.select(this);
    var dot = div.select("div.dot-file").text();
    var stageId = div.attr("name");
    if (forJob) {
      // TODO: handle attempts
      var stageLink = "/stages/stage/?id=" + stageId.replace(/^stage/, "") + "&attempt=0";
      var parentContainer = svg.append("a").attr("xlink:href", stageLink);
    } else {
      var parentContainer = svg;
    }
    var container = parentContainer.append("g").attr("id", "graph_" + stageId);
    // No need to shift the first stage container
    if (i > 0) {
      // Move the container so it doesn't overlap with the existing ones
      // To do this, first we find the position and width of the last stage's container
      var existingStages = d3.selectAll("svg g.cluster").filter(function() {
        var ourClusterId = "cluster_stage" + stageId;
        var theirId = d3.select(this).attr("id");
        return theirId.indexOf("cluster_stage") > -1 && theirId != ourClusterId;
      });
      if (!existingStages.empty()) {
        var lastStageId = d3.select(existingStages[0].pop()).attr("id");
        var lastStageWidth = toFloat(d3.select("#" + lastStageId + " rect").attr("width"));
        var lastStagePosition = getAbsolutePosition(lastStageId);
        var offset = lastStagePosition.x + lastStageWidth + VizConstants.stageSep;
        container.attr("transform", "translate(" + offset + ", 0)");
      }
    }
    renderDot(dot, container);
    // If there are any incoming edges into this graph, keep track of them to
    // render them separately later (job page only). Note that we cannot draw
    // them now because we need to put these edges in a container that is on
    // top of all stage graphs.
    if (forJob) {
      div.selectAll("div.incoming-edge").each(function(v) {
        var edge = d3.select(this).text().split(","); // e.g. 3,4 => [3, 4]
        crossStageEdges.push(edge);
      });
    }
  });

  // Time to draw cross stage edges (job page only)!
  if (crossStageEdges.length > 0 && forJob) {
    var container = svg.append("g").attr("id", "cross-stage-edges");
    for (var i = 0; i < crossStageEdges.length; i++) {
      var fromId = "node_" + crossStageEdges[i][0];
      var toId = "node_" + crossStageEdges[i][1];
      connect(fromId, toId, container);
    }
  }

  // Set the appropriate SVG dimensions to ensure that all elements are displayed
  var boundingBox = svg.node().getBBox();
  svg.style("width", (boundingBox.width + VizConstants.svgMarginX) + "px");
  svg.style("height", (boundingBox.height + VizConstants.svgMarginY) + "px"); 

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
  var startX = -VizConstants.svgMarginX;
  var startY = -VizConstants.svgMarginY;
  var endX = toFloat(svg.style("width")) + VizConstants.svgMarginX;
  var endY = toFloat(svg.style("height")) + VizConstants.svgMarginY;
  var newViewBox = startX + " " + startY + " " + endX + " " + endY;
  svg.attr("viewBox", newViewBox);
}

/* Render the dot file as an SVG in the given container. */
function renderDot(dot, container) {
  var escaped_dot = dot
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, "\"");
  var g = graphlibDot.read(escaped_dot);
  var renderer = new dagreD3.render();
  renderer(container, g);
}

/* Style the visualization we just rendered. */
function styleViz(forJob) {
  d3.selectAll("svg g.cluster rect")
    .style("fill", "white")
    .style("stroke", VizConstants.rddScopeColor)
    .style("stroke-width", "4px")
    .style("stroke-opacity", "0.5");
  d3.selectAll("svg g.cluster text")
    .attr("fill", VizConstants.scopeLabelColor)
    .attr("font-size", "11px")
  d3.selectAll("svg path")
    .style("stroke", VizConstants.edgeColor)
    .style("stroke-width", VizConstants.edgeWidth);
  styleEdgeArrow();

  // Apply a different color to stage clusters
  d3.selectAll("svg g.cluster")
    .filter(function() {
      var name = d3.select(this).attr("name");
      return name && name.indexOf("Stage") > -1;
    })
    .select("rect")
    .style("stroke", VizConstants.stageScopeColor)
    .style("strokeWidth", "6px");

  // Apply any job or stage specific styles
  if (forJob) {
    styleJobViz();
  } else {
    styleStageViz();
  }
}

/*
 * Put an arrow at the end of every edge.
 * We need to do this because we manually render some edges ourselves through d3.
 * For these edges, we borrow the arrow marker generated by dagre-d3.
 */
function styleEdgeArrow() {
  var dagreD3Marker = d3.select("svg g.edgePaths marker").node()
  d3.select("svg")
    .append(function() { return dagreD3Marker.cloneNode(true); })
    .attr("id", "marker-arrow")
    .select("path")
    .attr("fill", VizConstants.edgeColor)
    .attr("strokeWidth", "0px");
  d3.selectAll("svg g > path").attr("marker-end", "url(#marker-arrow)")
  d3.selectAll("svg g.edgePaths def").remove(); // We no longer need these
}

/* Apply job-page-specific style to the visualization. */
function styleJobViz() {
  d3.selectAll("svg g.node circle")
    .style("fill", VizConstants.rddDotColor);
  d3.selectAll("svg g#cross-stage-edges path")
    .style("fill", "none");
}

/* Apply stage-page-specific style to the visualization. */
function styleStageViz() {
  d3.selectAll("svg g.node rect")
    .style("fill", "none")
    .style("stroke", VizConstants.rddColor)
    .style("stroke-width", "2px")
    .style("fill-opacity", "0.8")
    .style("stroke-opacity", "0.9");
  d3.selectAll("svg g.label text tspan")
    .style("fill", VizConstants.rddColor);
}

/*
 * (Job page only) Return the absolute position of the
 * group element identified by the given selector.
 */
function getAbsolutePosition(groupId) {
  var obj = d3.select("#" + groupId).filter("g");
  var _x = 0, _y = 0;
  while (!obj.empty()) {
    var transformText = obj.attr("transform");
    var translate = d3.transform(transformText).translate
    _x += translate[0];
    _y += translate[1];
    obj = d3.select(obj.node().parentNode).filter("g")
  }
  return { x: _x, y: _y };
}

/*
 * (Job page only) Connect two group elements with a curved edge.
 * This assumes that the path will be styled later.
 */
function connect(fromNodeId, toNodeId, container) {
  var from = getAbsolutePosition(fromNodeId);
  var to = getAbsolutePosition(toNodeId);

  // Account for node radius (this is highly-specific to the job page)
  // Otherwise the arrow heads will bleed into the circle itself
  var delta = toFloat(d3.select("svg g.nodes circle").attr("r"));
  var markerEndDelta = 2; // adjust for arrow stroke width
  if (from.x < to.x) {
    from.x = from.x + delta;
    to.x = to.x - delta - markerEndDelta;
  } else if (from.x > to.x) {
    from.x = from.x - delta;
    to.x = to.x + delta + markerEndDelta;
  }

  if (from.y == to.y) {
    // If they are on the same rank, curve the middle part of the edge
    // upward a little to avoid interference with things in between
    var points = [
      [from.x, from.y],
      [from.x + (to.x - from.x) * 0.2, from.y],
      [from.x + (to.x - from.x) * 0.3, from.y - 20],
      [from.x + (to.x - from.x) * 0.7, from.y - 20],
      [from.x + (to.x - from.x) * 0.8, to.y],
      [to.x, to.y]
    ];
  } else {
    var points = [
      [from.x, from.y],
      [from.x + (to.x - from.x) * 0.4, from.y],
      [from.x + (to.x - from.x) * 0.6, to.y],
      [to.x, to.y]
    ];
  }

  var line = d3.svg.line().interpolate("basis");
  container.append("path")
    .datum(points)
    .attr("d", line);
}

/* Helper method to convert attributes to numeric values. */
function toFloat(f) {
  return parseFloat(f.replace(/px$/, ""))
}

