/* Dynamically injected post-processing code for the API docs */

$(document).ready(function() {
  // Find annotations
  var annotations = $("dt:contains('Annotations')").next("dd").children("span.name")
  var alphaComponentElements = annotations.children("a[name='org.apache.spark.annotations.AlphaComponent']")
  var developerAPIElements = annotations.children("a[name='org.apache.spark.annotations.DeveloperAPI']")
  var experimentalElements = annotations.children("a[name='org.apache.spark.annotations.Experimental']")

  // Insert badges into DOM tree
  var alphaComponentHTML = "<span class='experimental badge'>ALPHA COMPONENT</span>"
  var developerAPIHTML = "<span class='developer badge'>Developer API</span>"
  var experimentalHTML = "<span class='experimental badge'>Experimental</span>"
  alphaComponentElements.closest("div.fullcomment").prevAll("h4.signature").prepend(alphaComponentHTML)
  alphaComponentElements.closest("div.fullcommenttop").prepend(alphaComponentHTML)
  developerAPIElements.closest("div.fullcomment").prevAll("h4.signature").prepend(developerAPIHTML)
  developerAPIElements.closest("div.fullcommenttop").prepend(developerAPIHTML)
  experimentalElements.closest("div.fullcomment").prevAll("h4.signature").prepend(experimentalHTML)
  experimentalElements.closest("div.fullcommenttop").prepend(experimentalHTML)
});

