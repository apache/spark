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

/*
 * This file contains the logic to render the RDD DAG visualization in the UI.
 *
 * This DAG describes the relationships between
 *   (1) an RDD and its dependencies,
 *   (2) an RDD and its operation scopes, and
 *   (3) an RDD's operation scopes and the stage / job hierarchy
 *
 * An operation scope is a general, named code block representing an operation
 * that instantiates RDDs (e.g. filter, textFile, reduceByKey). An operation
 * scope can be nested inside of other scopes if the corresponding RDD operation
 * invokes other such operations (for more detail, see o.a.s.rdd.operationScope).
 *
 * A stage may include one or more operation scopes if the RDD operations are
 * streamlined into one stage (e.g. rdd.map(...).filter(...).flatMap(...)).
 * On the flip side, an operation scope may also include one or many stages,
 * or even jobs if the RDD operation is higher level than Spark's scheduling
 * primitives (e.g. take, any SQL query).
 *
 * In the visualization, an RDD is expressed as a node, and its dependencies
 * as directed edges (from parent to child). operation scopes, stages, and
 * jobs are expressed as clusters that may contain one or many nodes. These
 * clusters may be nested inside of each other in the scenarios described
 * above.
 *
 * The visualization is rendered in an SVG contained in "div#dag-viz-graph",
 * and its input data is expected to be populated in "div#dag-viz-metadata"
 * by Spark's UI code. This is currently used only on the stage page and on
 * the job page.
 *
 * This requires jQuery, d3, and dagre-d3. Note that we use a custom release
 * of dagre-d3 (http://github.com/andrewor14/dagre-d3) for some specific
 * functionality. For more detail, please track the changes in that project
 * since it was forked (commit 101503833a8ce5fe369547f6addf3e71172ce10b).
 */

var VizConstants = {
  rddColor: "#444444",
  rddCachedColor: "#FF0000",
  rddOperationColor: "#AADFFF",
  stageColor: "#FFDDEE",
  clusterLabelColor: "#888888",
  edgeColor: "#444444",
  edgeWidth: "1.5px",
  svgMarginX: 0,
  svgMarginY: 20,
  stageSep: 50,
  graphPrefix: "graph_",
  nodePrefix: "node_",
  stagePrefix: "stage_",
  clusterPrefix: "cluster_",
  stageClusterPrefix: "cluster_stage_"
};

// Helper d3 accessors for the elements that contain our graph and its metadata
function graphContainer() { return d3.select("#dag-viz-graph"); }
function metadataContainer() { return d3.select("#dag-viz-metadata"); }

/*
 * Show or hide the RDD DAG visualization.
 * The graph is only rendered the first time this is called.
 */
function toggleDagViz(forJob) {
  var arrowSelector = ".expand-dag-viz-arrow";
  $(arrowSelector).toggleClass('arrow-closed');
  $(arrowSelector).toggleClass('arrow-open');
  var shouldShow = $(arrowSelector).hasClass("arrow-open");
  if (shouldShow) {
    var shouldRender = graphContainer().select("svg").empty();
    if (shouldRender) {
      renderDagViz(forJob);
    }
    graphContainer().style("display", "block");
  } else {
    // Save the graph for later so we don't have to render it again
    graphContainer().style("display", "none");
  }
}

/*
 * Render the RDD DAG visualization.
 *
 * Input DOM hierarchy:
 *   div#dag-viz-metadata >
 *   div.stage-metadata >
 *   div.[dot-file | incoming-edge | outgoing-edge]
 *
 * Output DOM hierarchy:
 *   div#dag-viz-graph >
 *   svg >
 *   g#cluster_stage_[stageId]
 *
 * Note that the input metadata is populated by o.a.s.ui.UIUtils.showDagViz.
 * Any changes in the input format here must be reflected there.
 */
function renderDagViz(forJob) {

  // If there is not a dot file to render, fail fast and report error
  if (metadataContainer().empty()) {
    graphContainer().append("div").text(
      "No visualization information available for this " + (forJob ? "job" : "stage"));
    return;
  }

  var svg = graphContainer().append("svg");
  if (forJob) {
    renderDagVizForJob(svg);
  } else {
    renderDagVizForStage(svg);
  }

  // Find cached RDDs
  metadataContainer().selectAll(".cached-rdd").each(function(v) {
    var nodeId = VizConstants.nodePrefix + d3.select(this).text();
    graphContainer().selectAll("#" + nodeId).classed("cached", true);
  });

  // Set the appropriate SVG dimensions to ensure that all elements are displayed
  var boundingBox = svg.node().getBBox();
  svg.style("width", (boundingBox.width + VizConstants.svgMarginX) + "px");
  svg.style("height", (boundingBox.height + VizConstants.svgMarginY) + "px");

  // Add labels to clusters because dagre-d3 doesn't do this for us
  svg.selectAll("g.cluster rect").each(function() {
    var rect = d3.select(this);
    var cluster = d3.select(this.parentNode);
    // Shift the boxes up a little to make room for the labels
    rect.attr("y", toFloat(rect.attr("y")) - 10);
    rect.attr("height", toFloat(rect.attr("height")) + 10);
    var labelX = toFloat(rect.attr("x")) + toFloat(rect.attr("width")) - 5;
    var labelY = toFloat(rect.attr("y")) + 15;
    var labelText = cluster.attr("name").replace(VizConstants.clusterPrefix, "");
    cluster.append("text")
      .attr("x", labelX)
      .attr("y", labelY)
      .attr("text-anchor", "end")
      .text(labelText);
  });

  // We have shifted a few elements upwards, so we should fix the SVG views
  var startX = -VizConstants.svgMarginX;
  var startY = -VizConstants.svgMarginY;
  var endX = toFloat(svg.style("width")) + VizConstants.svgMarginX;
  var endY = toFloat(svg.style("height")) + VizConstants.svgMarginY;
  var newViewBox = startX + " " + startY + " " + endX + " " + endY;
  svg.attr("viewBox", newViewBox);

  // Lastly, apply some custom style to the DAG
  styleDagViz(forJob);
}

/* Render the RDD DAG visualization for a stage. */
function renderDagVizForStage(svgContainer) {
  var metadata = metadataContainer().select(".stage-metadata");
  var dot = metadata.select(".dot-file").text();
  var containerId = VizConstants.graphPrefix + metadata.attr("stageId");
  var container = svgContainer.append("g").attr("id", containerId);
  renderDot(dot, container);
}

/*
 * Render the RDD DAG visualization for a job.
 *
 * Due to limitations in dagre-d3, each stage is rendered independently so that
 * we have more control on how to position them. Unfortunately, this means we
 * cannot rely on dagre-d3 to render edges that cross stages and must render
 * these manually on our own.
 */
function renderDagVizForJob(svgContainer) {
  var crossStageEdges = [];

  metadataContainer().selectAll(".stage-metadata").each(function(d, i) {
    var metadata = d3.select(this);
    var dot = metadata.select(".dot-file").text();
    var stageId = metadata.attr("stageId");
    var containerId = VizConstants.graphPrefix + stageId;
    // TODO: handle stage attempts
    var stageLink =
      "/stages/stage/?id=" + stageId.replace(VizConstants.stagePrefix, "") + "&attempt=0";
    var container = svgContainer
      .append("a").attr("xlink:href", stageLink)
      .append("g").attr("id", containerId);
    // Now we need to shift the container for this stage so it doesn't overlap
    // with existing ones. We do not need to do this for the first stage.
    if (i > 0) {
      // Take into account the position and width of the last stage's container
      var existingStages = stageClusters();
      if (!existingStages.empty()) {
        var lastStage = existingStages[0].pop();
        var lastStageId = d3.select(lastStage).attr("id");
        var lastStageWidth = toFloat(d3.select("#" + lastStageId + " rect").attr("width"));
        var lastStagePosition = getAbsolutePosition(lastStageId);
        var offset = lastStagePosition.x + lastStageWidth + VizConstants.stageSep;
        container.attr("transform", "translate(" + offset + ", 0)");
      }
    }
    renderDot(dot, container);
    // If there are any incoming edges into this graph, keep track of them to render
    // them separately later. Note that we cannot draw them now because we need to
    // put these edges in a separate container that is on top of all stage graphs.
    metadata.selectAll(".incoming-edge").each(function(v) {
      var edge = d3.select(this).text().split(","); // e.g. 3,4 => [3, 4]
      crossStageEdges.push(edge);
    });
  });

  // Draw edges that cross stages
  if (crossStageEdges.length > 0) {
    var container = svgContainer.append("g").attr("id", "cross-stage-edges");
    for (var i = 0; i < crossStageEdges.length; i++) {
      var fromRDDId = crossStageEdges[i][0];
      var toRDDId = crossStageEdges[i][1];
      connectRDDs(fromRDDId, toRDDId, container);
    }
  }
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
function styleDagViz(forJob) {
  graphContainer().selectAll("svg g.cluster rect")
    .style("fill", "white")
    .style("stroke", VizConstants.rddOperationColor)
    .style("stroke-width", "4px")
    .style("stroke-opacity", "0.5");
  graphContainer().selectAll("svg g.cluster text")
    .attr("fill", VizConstants.clusterLabelColor)
    .attr("font-size", "11px");
  graphContainer().selectAll("svg path")
    .style("stroke", VizConstants.edgeColor)
    .style("stroke-width", VizConstants.edgeWidth);
  stageClusters()
    .select("rect")
    .style("stroke", VizConstants.stageColor)
    .style("strokeWidth", "6px");

  // Put an arrow at the end of every edge
  // We need to do this because we manually render some edges ourselves
  // For these edges, we borrow the arrow marker generated by dagre-d3
  var dagreD3Marker = graphContainer().select("svg g.edgePaths marker").node();
  graphContainer().select("svg")
    .append(function() { return dagreD3Marker.cloneNode(true); })
    .attr("id", "marker-arrow")
    .select("path")
    .attr("fill", VizConstants.edgeColor)
    .attr("strokeWidth", "0px");
  graphContainer().selectAll("svg g > path").attr("marker-end", "url(#marker-arrow)");
  graphContainer().selectAll("svg g.edgePaths def").remove(); // We no longer need these

  // Apply any job or stage specific styles
  if (forJob) {
    styleDagVizForJob();
  } else {
    styleDagVizForStage();
  }
}

/* Apply job-page-specific style to the visualization. */
function styleDagVizForJob() {
  graphContainer().selectAll("svg g.node circle")
    .style("fill", VizConstants.rddColor);
  // TODO: add a legend to explain what a highlighted dot means
  graphContainer().selectAll("svg g.cached circle")
    .style("fill", VizConstants.rddCachedColor);
  graphContainer().selectAll("svg g#cross-stage-edges path")
    .style("fill", "none");
}

/* Apply stage-page-specific style to the visualization. */
function styleDagVizForStage() {
  graphContainer().selectAll("svg g.node rect")
    .style("fill", "none")
    .style("stroke", VizConstants.rddColor)
    .style("stroke-width", "2px")
    .attr("rx", "5") // round corners
    .attr("ry", "5");
    // TODO: add a legend to explain what a highlighted RDD means
  graphContainer().selectAll("svg g.cached rect")
    .style("stroke", VizConstants.rddCachedColor);
  graphContainer().selectAll("svg g.node g.label text tspan")
    .style("fill", VizConstants.rddColor);
}

/*
 * (Job page only) Helper method to compute the absolute
 * position of the group element identified by the given ID.
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

/* (Job page only) Connect two RDD nodes with a curved edge. */
function connectRDDs(fromRDDId, toRDDId, container) {
  var fromNodeId = VizConstants.nodePrefix + fromRDDId;
  var toNodeId = VizConstants.nodePrefix + toRDDId
  var fromPos = getAbsolutePosition(fromNodeId);
  var toPos = getAbsolutePosition(toNodeId);

  // On the job page, RDDs are rendered as dots (circles). When rendering the path,
  // we need to account for the radii of these circles. Otherwise the arrow heads
  // will bleed into the circle itself.
  var delta = toFloat(graphContainer()
    .select("g.node#" + toNodeId)
    .select("circle")
    .attr("r"));
  if (fromPos.x < toPos.x) {
    fromPos.x += delta;
    toPos.x -= delta;
  } else if (fromPos.x > toPos.x) {
    fromPos.x -= delta;
    toPos.x += delta;
  }

  if (fromPos.y == toPos.y) {
    // If they are on the same rank, curve the middle part of the edge
    // upward a little to avoid interference with things in between
    // e.g.       _______
    //      _____/       \_____
    var points = [
      [fromPos.x, fromPos.y],
      [fromPos.x + (toPos.x - fromPos.x) * 0.2, fromPos.y],
      [fromPos.x + (toPos.x - fromPos.x) * 0.3, fromPos.y - 20],
      [fromPos.x + (toPos.x - fromPos.x) * 0.7, fromPos.y - 20],
      [fromPos.x + (toPos.x - fromPos.x) * 0.8, toPos.y],
      [toPos.x, toPos.y]
    ];
  } else {
    // Otherwise, draw a curved edge that flattens out on both ends
    // e.g.       _____
    //           /
    //          |
    //    _____/
    var points = [
      [fromPos.x, fromPos.y],
      [fromPos.x + (toPos.x - fromPos.x) * 0.4, fromPos.y],
      [fromPos.x + (toPos.x - fromPos.x) * 0.6, toPos.y],
      [toPos.x, toPos.y]
    ];
  }

  var line = d3.svg.line().interpolate("basis");
  container.append("path").datum(points).attr("d", line);
}

/* Helper d3 accessor to clusters that represent stages. */
function stageClusters() {
  return graphContainer().selectAll("g.cluster").filter(function() {
    return d3.select(this).attr("id").indexOf(VizConstants.stageClusterPrefix) > -1;
  });
}

/* Helper method to convert attributes to numeric values. */
function toFloat(f) {
  return parseFloat(f.replace(/px$/, ""));
}

