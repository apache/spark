/* Dynamically injected post-processing code for the API docs */

$(document).ready(function() {
  var annotations = $("dt:contains('Annotations')").next("dd").children("span.name");
  addBadges(annotations, "AlphaComponent", ":: AlphaComponent ::", "<span class='experimental badge'>ALPHA COMPONENT</span>");
  addBadges(annotations, "DeveloperApi", ":: DeveloperApi ::", "<span class='developer badge'>Developer API</span>");
  addBadges(annotations, "Experimental", ":: Experimental ::", "<span class='experimental badge'>Experimental</span>");
});

function addBadges(allAnnotations, name, tag, html) {
  var fullName = "org.apache.spark.annotations." + name;
  var annotations = allAnnotations.children("a[name='" + fullName + "']");
  var tags = $("p.comment:contains(" + tag + ")").add(
    $("div.comment p:contains(" + tag + ")"));

  // Remove identifier tags from comments
  tags.each(function(index) {
    var oldHTML = $(this).html();
    var newHTML = oldHTML.replace(tag, "");
    $(this).html(newHTML);
  });

  // Add badges to all containers
  tags.prevAll("h4.signature").prepend(html);
  annotations.closest("div.fullcomment").prevAll("h4.signature").prepend(html);
  annotations.closest("div.fullcommenttop").prepend(html);
}
