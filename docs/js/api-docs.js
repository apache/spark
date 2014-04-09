/* Dynamically injected post-processing code for the API docs */

$(document).ready(function() {
  var annotations = $("dt:contains('Annotations')").next("dd").children("span.name");
  addBadges(annotations, "AlphaComponent", ":: AlphaComponent ::", "<span class='alphaComponent badge'>Alpha Component</span>");
  addBadges(annotations, "DeveloperApi", ":: DeveloperApi ::", "<span class='developer badge'>Developer API</span>");
  addBadges(annotations, "Experimental", ":: Experimental ::", "<span class='experimental badge'>Experimental</span>");
});

function addBadges(allAnnotations, name, tag, html) {
  var annotations = allAnnotations.filter(":contains('" + name + "')")
  var tags = $(".cmt:contains(" + tag + ")")

  // Remove identifier tags from comments
  tags.each(function(index) {
    var oldHTML = $(this).html();
    var newHTML = oldHTML.replace(tag, "");
    $(this).html(newHTML);
  });

  // Add badges to all containers
  tags.prevAll("h4.signature")
    .add(annotations.closest("div.fullcommenttop"))
    .add(annotations.closest("div.fullcomment").prevAll("h4.signature"))
    .prepend(html);
}
