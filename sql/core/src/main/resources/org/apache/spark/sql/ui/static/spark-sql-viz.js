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

var PlanVizConstants = {
  svgMarginX: 16,
  svgMarginY: 16
};

function renderPlanViz() {
  var svg = planVizContainer().append("svg");
  var metadata = d3.select("#plan-viz-metadata");
  var dot = metadata.select(".dot-file").text().trim();
  var graph = svg.append("g");

  var g = graphlibDot.read(dot);
  preprocessGraphLayout(g);
  var renderer = new dagreD3.render();
  renderer(graph, g);

  // Round corners on rectangles
  svg
    .selectAll("rect")
    .attr("rx", "5")
    .attr("ry", "5");

  var nodeSize = parseInt($("#plan-viz-metadata-size").text());
  for (var i = 0; i < nodeSize; i++) {
    setupTooltipForSparkPlanNode(i);
  }

  resizeSvg(svg)
}

/* -------------------- *
 * | Helper functions | *
 * -------------------- */

function planVizContainer() { return d3.select("#plan-viz-graph"); }

/*
 * Set up the tooltip for a SparkPlan node using metadata. When the user moves the mouse on the
 * node, it will display the details of this SparkPlan node in the right.
 */
function setupTooltipForSparkPlanNode(nodeId) {
  var nodeTooltip = d3.select("#plan-meta-data-" + nodeId).text()
  d3.select("svg g .node_" + nodeId)
    .on('mouseover', function(d) {
      var domNode = d3.select(this).node();
      $(domNode).tooltip({
        title: nodeTooltip, trigger: "manual", container: "body", placement: "right"
      });
      $(domNode).tooltip("show");
    })
    .on('mouseout', function(d) {
      var domNode = d3.select(this).node();
      $(domNode).tooltip("destroy");
    })
}

/*
 * Helper function to pre-process the graph layout.
 * This step is necessary for certain styles that affect the positioning
 * and sizes of graph elements, e.g. padding, font style, shape.
 */
function preprocessGraphLayout(g) {
  var nodes = g.nodes();
  for (var i = 0; i < nodes.length; i++) {
      var node = g.node(nodes[i]);
      node.padding = "5";
  }
  // Curve the edges
  var edges = g.edges();
  for (var j = 0; j < edges.length; j++) {
    var edge = g.edge(edges[j]);
    edge.lineInterpolate = "basis";
  }
}

/*
 * Helper function to size the SVG appropriately such that all elements are displayed.
 * This assumes that all outermost elements are clusters (rectangles).
 */
function resizeSvg(svg) {
  var allClusters = svg.selectAll("g rect")[0];
  console.log(allClusters);
  var startX = -PlanVizConstants.svgMarginX +
    toFloat(d3.min(allClusters, function(e) {
      console.log(e);
      return getAbsolutePosition(d3.select(e)).x;
    }));
  var startY = -PlanVizConstants.svgMarginY +
    toFloat(d3.min(allClusters, function(e) {
      return getAbsolutePosition(d3.select(e)).y;
    }));
  var endX = PlanVizConstants.svgMarginX +
    toFloat(d3.max(allClusters, function(e) {
      var t = d3.select(e);
      return getAbsolutePosition(t).x + toFloat(t.attr("width"));
    }));
  var endY = PlanVizConstants.svgMarginY +
    toFloat(d3.max(allClusters, function(e) {
      var t = d3.select(e);
      return getAbsolutePosition(t).y + toFloat(t.attr("height"));
    }));
  var width = endX - startX;
  var height = endY - startY;
  svg.attr("viewBox", startX + " " + startY + " " + width + " " + height)
     .attr("width", width)
     .attr("height", height);
}

/* Helper function to convert attributes to numeric values. */
function toFloat(f) {
  if (f) {
    return parseFloat(f.toString().replace(/px$/, ""));
  } else {
    return f;
  }
}

/*
 * Helper function to compute the absolute position of the specified element in our graph.
 */
function getAbsolutePosition(d3selection) {
  if (d3selection.empty()) {
    throw "Attempted to get absolute position of an empty selection.";
  }
  var obj = d3selection;
  var _x = toFloat(obj.attr("x")) || 0;
  var _y = toFloat(obj.attr("y")) || 0;
  while (!obj.empty()) {
    var transformText = obj.attr("transform");
    if (transformText) {
      var translate = d3.transform(transformText).translate;
      _x += toFloat(translate[0]);
      _y += toFloat(translate[1]);
    }
    // Climb upwards to find how our parents are translated
    obj = d3.select(obj.node().parentNode);
    // Stop when we've reached the graph container itself
    if (obj.node() == planVizContainer().node()) {
      break;
    }
  }
  return { x: _x, y: _y };
}
