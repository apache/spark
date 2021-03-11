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

function shouldRenderPlanViz() {
  return planVizContainer().selectAll("svg").empty();
}

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

  resizeSvg(svg);
  postprocessForAdditionalMetrics();
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
  var nodeTooltip = d3.select("#plan-meta-data-" + nodeId).text();
  d3.select("svg g .node_" + nodeId)
    .each(function(d) {
      var domNode = d3.select(this).node();
      $(domNode).tooltip({
        title: nodeTooltip, trigger: "hover focus", container: "body", placement: "top"
      });
    })
}

// labelSeparator should be a non-graphical character in order not to affect the width of boxes.
var labelSeparator = "\x01";
var stageAndTaskMetricsPattern = "^(.*)(\\(stage.*task[^)]*\\))(.*)$";

/*
 * Helper function to pre-process the graph layout.
 * This step is necessary for certain styles that affect the positioning
 * and sizes of graph elements, e.g. padding, font style, shape.
 */
function preprocessGraphLayout(g) {
  g.graph().ranksep = "70";
  var nodes = g.nodes();
  for (var i = 0; i < nodes.length; i++) {
    var node = g.node(nodes[i]);
    node.padding = "5";

    var firstSeparator;
    var secondSeparator;
    var splitter;
    if (node.isCluster) {
      firstSeparator = secondSeparator = labelSeparator;
      splitter = "\\n";
    } else {
      firstSeparator = "<span class='stageId-and-taskId-metrics'>";
      secondSeparator = "</span>";
      splitter = "<br>";
    }

    node.label.split(splitter).forEach(function(text, i) {
      var newTexts = text.match(stageAndTaskMetricsPattern);
      if (newTexts) {
        node.label = node.label.replace(
            newTexts[0],
            newTexts[1] + firstSeparator + newTexts[2] + secondSeparator + newTexts[3]);
      }
    });
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
  var startX = -PlanVizConstants.svgMarginX +
    toFloat(d3.min(allClusters, function(e) {
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
    if (obj.node() === planVizContainer().node()) {
      break;
    }
  }
  return { x: _x, y: _y };
}

/*
 * Helper function for postprocess for additional metrics.
 */
function postprocessForAdditionalMetrics() {
  // With dagre-d3, we can choose normal text (default) or HTML as a label type.
  // HTML label for node works well but not for cluster so we need to choose the default label type
  // and manipulate DOM.
  $("g.cluster text tspan")
    .each(function() {
      var originalText = $(this).text();
        if (originalText.indexOf(labelSeparator) > 0) {
          var newTexts = originalText.split(labelSeparator);
          var thisD3Node = d3.selectAll($(this));
          thisD3Node.text(newTexts[0]);
          thisD3Node.append("tspan").attr("class", "stageId-and-taskId-metrics").text(newTexts[1]);
          $(this).append(newTexts[2]);
        } else {
          return originalText;
        }
  });

  var checkboxNode = $("#stageId-and-taskId-checkbox");
  checkboxNode.click(function() {
      onClickAdditionalMetricsCheckbox($(this));
  });
  var isChecked = window.localStorage.getItem("stageId-and-taskId-checked") === "true";
  checkboxNode.prop("checked", isChecked);
  onClickAdditionalMetricsCheckbox(checkboxNode);
}

/*
 * Helper function which defines the action on click the checkbox.
 */
function onClickAdditionalMetricsCheckbox(checkboxNode) {
  var additionalMetrics = $(".stageId-and-taskId-metrics");
  var isChecked = checkboxNode.prop("checked");
  if (isChecked) {
    additionalMetrics.show();
  } else {
    additionalMetrics.hide();
  }
  window.localStorage.setItem("stageId-and-taskId-checked", isChecked);
}
