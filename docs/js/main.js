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

/* Custom JavaScript code in the MarkDown docs */

// Enable language-specific code tabs
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


// A script to fix internal hash links because we have an overlapping top bar.
// Based on https://github.com/twitter/bootstrap/issues/193#issuecomment-2281510
function maybeScrollToHash() {
  if (window.location.hash && $(window.location.hash).length) {
    var newTop = $(window.location.hash).offset().top - 57;
    $(window).scrollTop(newTop);
  }
}

$(function() {
  codeTabs();
  // Display anchor links when hovering over headers. For documentation of the
  // configuration options, see the AnchorJS documentation.
  anchors.options = {
    placement: 'right'
  };
  anchors.add();

  $(window).bind('hashchange', function() {
    maybeScrollToHash();
  });

  // Scroll now too in case we had opened the page on a hash, but wait a bit because some browsers
  // will try to do *their* initial scroll after running the onReady handler.
  $(window).load(function() { setTimeout(function() { maybeScrollToHash(); }, 25); }); 
});
