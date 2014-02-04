
// From docs.scala-lang.org
function styleCode() {
  if (typeof disableStyleCode != "undefined") {
    return;
  }
  $(".codetabs pre code").parent().each(function() {
    if (!$(this).hasClass("prettyprint")) {
      var lang = $(this).parent().data("lang");
      if (lang == "python") {
        lang = "py"
      }
      if (lang == "bash") {
        lang = "bsh"
      }
      $(this).addClass("prettyprint lang-"+lang+" linenums");
    }
  });
  console.log("runningPrettyPrint()")
  prettyPrint();
}


function codeTabs() {
  var counter = 0;
  var langImages = {
    "scala": "img/scala-sm.png",
    "python": "img/python-sm.png",
    "java": "img/java-sm.png"
  };
  $("div.codetabs").each(function() {
    $(this).addClass("tab-content");

    // Insert the tab bar
    var tabBar = $('<ul class="nav nav-tabs" data-tabs="tabs"></ul>');
    $(this).before(tabBar);

    // Add each code sample to the tab bar:
    var codeSamples = $(this).children("div");
    codeSamples.each(function() {
      $(this).addClass("tab-pane");
      var lang = $(this).data("lang");
      var image = $(this).data("image");
      var notabs = $(this).data("notabs");
      var capitalizedLang = lang.substr(0, 1).toUpperCase() + lang.substr(1);
      var id = "tab_" + lang + "_" + counter;
      $(this).attr("id", id);
      if (image != null && langImages[lang]) {
        var buttonLabel = "<img src='" +langImages[lang] + "' alt='" + capitalizedLang + "' />";
      } else if (notabs == null) {
        var buttonLabel = "<b>" + capitalizedLang + "</b>";
      } else {
        var buttonLabel = ""
      }
      tabBar.append(
        '<li><a class="tab_' + lang + '" href="#' + id + '">' + buttonLabel + '</a></li>'
      );
    });

    codeSamples.first().addClass("active");
    tabBar.children("li").first().addClass("active");
    counter++;
  });
  $("ul.nav-tabs a").click(function (e) {
    // Toggling a tab should switch all tabs corresponding to the same language
    // while retaining the scroll position
    e.preventDefault();
    var scrollOffset = $(this).offset().top - $(document).scrollTop();
    $("." + $(this).attr('class')).tab('show');
    $(document).scrollTop($(this).offset().top - scrollOffset);
  });
}

function makeCollapsable(elt, accordionClass, accordionBodyId, title) {
  $(elt).addClass("accordion-inner");
  $(elt).wrap('<div class="accordion ' + accordionClass + '"></div>')
  $(elt).wrap('<div class="accordion-group"></div>')
  $(elt).wrap('<div id="' + accordionBodyId + '" class="accordion-body collapse"></div>')
  $(elt).parent().before(
    '<div class="accordion-heading">' +
      '<a class="accordion-toggle" data-toggle="collapse" href="#' + accordionBodyId + '">' +
             title +
      '</a>' +
    '</div>'
  );
}

function viewSolution() {
  var counter = 0
  $("div.solution").each(function() {
    var id = "solution_" + counter
    makeCollapsable(this, "", id,
      '<i class="icon-ok-sign" style="text-decoration: none; color: #0088cc">' +
      '</i>' + "View Solution");
    counter++;
  });
}


$(document).ready(function() {
  codeTabs();
  viewSolution();
  $('#chapter-toc').toc({exclude: '', context: '.container'});
  $('#chapter-toc').prepend('<p class="chapter-toc-header">In This Chapter</p>');
  makeCollapsable($('#global-toc'), "", "global-toc", "Show Table of Contents");
  //styleCode();
});
