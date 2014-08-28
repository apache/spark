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
    var WIDTH = 800, HEIGHT = 1000;
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
          if (!nodesMap[call]) {
            // new node
            var n = {name: call, stages: [cStage], children: null};
            nodes.push(n);
            nodesMap[n.name] = n;
          } else {
            nodesMap[call].stages.push(cStage);
          }

          if (prevNode !== null) {
            var link = {};
      
            link.source = nodesMap[call];
            link.target = prevNode;
            link.stage = cStage;
            
            link.target.parent = link.source.name;
            if (link.source.children) {
              var found = false;
              link.source.children.forEach(function(c) {
                if (c.name === link.target.name) {
                  found = true;
                }
              });
              if (!found) {
                link.source.children.push(link.target);
              }
            } else {
              link.source.children = [link.target];
            }

            graphData.push(link);
          }

          prevNode = nodesMap[call];
        }
      }
    }
    
    function shortenFunctionName(name) {
      return name.substring(name.indexOf("(") + 1, name.indexOf(")"))
    }

    var g = new dagreD3.Digraph();
    var color = d3.scale.category20();

    // States and transitions from RFC 793
    var states = [];
    for (var i = 0; i < nodes.length; i++) {
      var n = shortenFunctionName(nodes[i].name);
      states.push(n);
    }

    // Automatically label each of the nodes
    states.forEach(function(state) { g.addNode(state, { label: state }); });
    
    for (var i = 0; i < graphData.length; i++) {
      g.addEdge(null, shortenFunctionName(graphData[i].source.name),
                      shortenFunctionName(graphData[i].target.name),
                      { label: "", style: 'stroke-width: 1.5px; stroke: ' + color(graphData[i].stage) + ';' });
    }

    // Create the renderer
    var renderer = new dagreD3.Renderer();

    // Set up an SVG group so that we can translate the final graph.
    var svg = d3.select('#canvas-svg').append('svg'),
        svgGroup = svg.append('g');

    // Set initial zoom to 75%
    var initialScale = 1;
    var oldZoom = renderer.zoom();
    renderer.zoom(function(graph, svg) {
      var zoom = oldZoom(graph, svg);

      // We must set the zoom and then trigger the zoom event to synchronize
      // D3 and the DOM.
      zoom.scale(initialScale).event(svg);
      return zoom;
    });

    // Run the renderer. This is what draws the final graph.
    var layout = renderer.run(g, svgGroup);

    // Center the graph
    var svgWidth = layout.graph().width * initialScale + 40;
    var svgHeight = layout.graph().height * initialScale + 40
    svg.attr('width', svgWidth);
    svg.attr('height', svgHeight);
    
    // Fit to window
    var scaleX = window.innerWidth / svgWidth;
  
    if (scaleX < 1) {
      svg.select("g").attr("transform", "scale(" + scaleX * 0.9 + ", " + scaleX + ")");
      svgWidth = window.innerWidth * 0.9;
      svgHeight = svgHeight * scaleX;
      svg.attr('width', svgWidth);
      svg.attr('height', svgHeight);
    }

    // arror marker for legend
    var arrowMarkers = svg.append("g").attr("id", "arrow-markers");
    arrowMarkers.selectAll(".arrow-marker")
      .data(Object.keys(stagesData)).enter()
      .append("marker")
        .attr("xmlns", "http://www.w3.org/2000/svg")
        .attr("id", function(d) {
          return "triangle-" + d;
        })
        .attr("class", ".arrow-marker")
        .attr("viewBox", "0 0 10 10")
        .attr("refX", "8")
        .attr("refY", "5")
        .attr("markerUnits", "strokeWidth")
        .attr("markerWidth", "8")
        .attr("markerHeight", "6")
        .attr("orient", "auto")
        .style("fill", function(d) {
          return color(d);
        })
        .html('<path d="M 0 0 L 10 5 L 0 10 z"/>');

    var legend = svg.selectAll(".legend")
        .data(Object.keys(stagesData)).enter()
        .append("g").attr("class", "legend")
        .attr("transform", function(d, i) {
            return "translate(50," + (20 + i * 25) + ")";
        });

    legend.append("line")
        .attr("marker-end", function(d) {
          return "url(#triangle-" + d + ")";
        })
        .attr("x1", function(d, i) {
          return svgWidth - 250;
        })
        .attr("x2", function(d, i) {
          return svgWidth - 150;
        })
        .attr("y1", function(d, i) {
          return (10)
        })
        .attr("y2", function(d, i) {
          return (10)
        })
        .style("stroke", function(d) {
            return color(d);
        });
    
    legend.append("text").attr("x", svgWidth - 90)
        .attr("y", 9).attr("dy", ".35em")
        .style("text-anchor", "end").text(function(d) {
            return "Stage " + d;
        });
  }

}