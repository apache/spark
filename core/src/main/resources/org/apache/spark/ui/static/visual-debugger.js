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

visualDebugger = {

  htmlDecode: function(input){
    var e = document.createElement('div');
    e.innerHTML = input;
    return e.childNodes.length === 0 ? "" : e.childNodes[0].nodeValue;
  },
  drawD3Document: function(stagesData) {
    var WIDTH = 1000, HEIGHT = 800;
    var graphData = [];

    var nodes = [];
    var nodesMap = {};

    for (var i = 0; i < Object.keys(stagesData).length; i++) {
      var cStage = Object.keys(stagesData)[i];
      var stackData = stagesData[cStage];

      var callData = this.htmlDecode(stackData).split("\n");
      var prevNode = null;

      for (var j = 0; j < callData.length; j++) {
        var call = callData[j].trim();

        if (call !== "") {
          if (!nodesMap[callData[j]]) {
            // new node
            var n = {name: callData[j], stages: [cStage]};
            nodes.push(n);
            nodesMap[n.name] = n;
          } else {
            nodesMap[callData[j]].stages.push(cStage);
          }

          if (prevNode !== null) {
            var link = {};

            link.source = nodesMap[callData[j]];
            link.target = prevNode;

            graphData.push(link);
          }

          prevNode = nodesMap[callData[j]];
        }
      }
    }

    var width = WIDTH,
        height = HEIGHT;

    var force = d3.layout.force()
        .nodes(d3.values(nodes))
        .links(graphData)
        .size([width - 200, height])
        .linkDistance(100)
        .gravity(0.1)
        .charge(-1000)
        .on("tick", tick)
        .start();

    var svg = d3.select("#canvas-svg").append("svg")
        .attr("width", width)
        .attr("height", height);

    // build the arrow.
    svg.append("svg:defs").selectAll("marker")
        .data(["end"])      // Different link/path types can be defined here
      .enter().append("svg:marker")    // This section adds in the arrows
        .attr("id", String)
        .attr("viewBox", "0 -5 10 10")
        .attr("refX", 22)
        .attr("refY", -2.5)
        .attr("markerWidth", 10)
        .attr("markerHeight", 10)
        .attr("orient", "auto")
      .append("svg:path")
        .attr("d", "M0,-5L10,0L0,5");

    // add the links and the arrows
    var path = svg.append("svg:g").selectAll("path")
        .data(force.links())
      .enter().append("svg:path")
        .attr("class", "link")
        .attr("marker-end", "url(#end)");

    // define the nodes
    var node = svg.selectAll(".node")
        .data(force.nodes())
      .enter().append("g")
        .attr("class", function(d) {
          var cl = "circle";
          if (d.stages.length > 1) {
            cl = "pie";
          }
          return cl;
        })
        .call(force.drag);

    var color = d3.scale.category20();
    var radius = 20;

    // add circle nodes
    d3.selectAll("g.circle").append("circle")
        .attr("r", radius)
        .style("fill", function(d) {
          return color(d.stages[0]);
        });

    var arc = d3.svg.arc()
        .outerRadius(radius)
        .innerRadius(0);

    var pie = d3.layout.pie()
        .sort(null)
        .value(function(d) { return 10; });

    // add pie nodes
    d3.selectAll("g.pie").call(function(d) {
      for (var i = 0; i < d[0].length; i++) {
        var pie_arcs = d3.select(d[0][i]).selectAll(".arc")
            .data(pie(d.data()[i].stages))
            .enter().append("g")
            .attr("class", "arc");

        pie_arcs.append("path")
            .attr("d", arc)
            .style("fill", function(d) {
              return color(d.data);
            });
      }
    });

    // add the text
    node.append("text")
        .attr("x", 25)
        .attr("dy", ".35em")
        .text(function(d) {
          return d.name.substring(d.name.indexOf("(") + 1, d.name.indexOf(")"));
        });

    // add the curvy lines
    function tick() {
        path.attr("d", function(d) {
            var dx = d.target.x - d.source.x,
                dy = d.target.y - d.source.y,
                dr = Math.sqrt(dx * dx + dy * dy);
            return "M" +
                d.source.x + "," +
                d.source.y + "A" +
                dr + "," + dr + " 0 0,1 " +
                d.target.x + "," +
                d.target.y;
        });

        node.attr("transform", function(d) {
          return "translate(" + d.x + "," + d.y + ")"; });
    }

    // append legend
    var legend = svg.selectAll(".legend")
        .data(Object.keys(stagesData)).enter()
        .append("g").attr("class", "legend")
        .attr("transform", function(d, i) {
            return "translate(50," + (70 + i * 25) + ")";
        });

    legend.append("rect")
        .attr("x", width - 200)
        .attr("width", 18).attr("height", 18)
        .style("fill", function(d) {
            return color(d);
        });
    legend.append("text").attr("x", width - 210)
        .attr("y", 9).attr("dy", ".35em")
        .style("text-anchor", "end").text(function(d) {
            return "Stage " + d;
        });
  }

}