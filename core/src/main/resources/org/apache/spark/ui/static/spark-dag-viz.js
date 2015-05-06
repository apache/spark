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
  svgMarginX: 20,
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
  var jobOrStage = forJob ? "job" : "stage";
  if (metadataContainer().empty()) {
    graphContainer().append("div").text(
      "No visualization information available for this " + jobOrStage);
    return;
  }

  var svg = graphContainer().append("svg").attr("class", jobOrStage);

  if (forJob) {
    renderDagVizForJob(svg);
    postProcessDagVizForJob();
  } else {
    renderDagVizForStage(svg);
    postProcessDagVizForStage();
  }

  // Find cached RDDs
  metadataContainer().selectAll(".cached-rdd").each(function(v) {
    var nodeId = VizConstants.nodePrefix + d3.select(this).text();
    graphContainer().selectAll("#" + nodeId).classed("cached", true);
  });

  //
  createClusterLabels(svg, forJob);

  //
  resizeSvg(svg);
}

function postProcessDagVizForJob() {
}

function postProcessDagVizForStage() {
  // Round corners on RDDs on the stage page
  graphContainer()
    .selectAll("svg.stage g.node rect")
    .attr("rx", "5")
    .attr("ry", "5");
}

/*
 *
 */
function createClusterLabels(svg, forJob) {
  var extraSpace = forJob ? 10 : 20;
  svg.selectAll("g.cluster rect").each(function() {
    var rect = d3.select(this);
    var cluster = d3.select(this.parentNode);
    // Shift the boxes up a little to make room for the labels
    rect.attr("y", toFloat(rect.attr("y")) - extraSpace);
    rect.attr("height", toFloat(rect.attr("height")) + extraSpace);
    var labelX = toFloat(rect.attr("x")) + toFloat(rect.attr("width")) - extraSpace / 2;
    var labelY = toFloat(rect.attr("y")) + extraSpace / 2 + 10;
    var labelText = cluster.attr("name").replace(VizConstants.clusterPrefix, "");
    cluster.append("text")
      .attr("x", labelX)
      .attr("y", labelY)
      .attr("text-anchor", "end")
      .text(labelText);
  });
}

/*
 * Helper method to size the SVG appropriately such that all elements are displyed.
 * This assumes that all nodes are embeded in clusters (rectangles).
 */
function resizeSvg(svg) {
  var allClusters = svg.selectAll("g.cluster rect")[0];
  var startX = -VizConstants.svgMarginX +
    toFloat(d3.min(allClusters, function(e) {
      return getAbsolutePosition(d3.select(e)).x;
    }));
  var startY = -VizConstants.svgMarginY +
    toFloat(d3.min(allClusters, function(e) {
      return getAbsolutePosition(d3.select(e)).y;
    }));
  var endX = VizConstants.svgMarginX +
    toFloat(d3.max(allClusters, function(e) {
      var t = d3.select(e)
      return getAbsolutePosition(t).x + toFloat(t.attr("width"));
    }));
  var endY = VizConstants.svgMarginY +
    toFloat(d3.max(allClusters, function(e) {
      var t = d3.select(e)
      return getAbsolutePosition(t).y + toFloat(t.attr("height"));
    }));
  var width = endX - startX;
  var height = endY - startY;
  svg.attr("viewBox", startX + " " + startY + " " + width + " " + height)
     .attr("width", width)
     .attr("height", height);
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
    // Set up container
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
      var existingStages = graphContainer()
        .selectAll("svg g.cluster")
        .filter("[id*=\"" + VizConstants.stageClusterPrefix + "\"]");
      if (!existingStages.empty()) {
        var lastStage = d3.select(existingStages[0].pop());
        var lastStageId = lastStage.attr("id");
        var lastStageWidth = toFloat(graphContainer()
          .select("#" + lastStageId)
          .select("rect")
          .attr("width"));
        var lastStagePosition = getAbsolutePosition(lastStage);
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

  // Put an arrow at the end of every edge
  // We need to do this because we manually render some edges ourselves
  // For these edges, we borrow the arrow marker generated by dagre-d3
  var dagreD3Marker = graphContainer().select("svg g.edgePaths marker").node();
  graphContainer().select("svg")
    .append(function() { return dagreD3Marker.cloneNode(true); })
    .attr("id", "marker-arrow")
  graphContainer().selectAll("svg g > path").attr("marker-end", "url(#marker-arrow)");
  graphContainer().selectAll("svg g.edgePaths def").remove(); // We no longer need these
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

/*
 * (Job page only) Helper method to compute the absolute
 * position of the specified element in our graph.
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
    if (obj.node() == graphContainer().node()) {
      break;
    }
  }
  return { x: _x, y: _y };
}

/* (Job page only) Connect two RDD nodes with a curved edge. */
function connectRDDs(fromRDDId, toRDDId, container) {
  var fromNodeId = VizConstants.nodePrefix + fromRDDId;
  var toNodeId = VizConstants.nodePrefix + toRDDId;
  var fromPos = getAbsolutePosition(graphContainer().select("#" + fromNodeId));
  var toPos = getAbsolutePosition(graphContainer().select("#" + toNodeId));

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

/* Helper method to convert attributes to numeric values. */
function toFloat(f) {
  if (f) {
    return parseFloat(f.toString().replace(/px$/, ""));
  } else {
    return f;
  }
}

