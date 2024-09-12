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

/* global $, anchors */

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
    var tabBar = $('<ul class="nav nav-tabs mb-3" data-tabs="tabs" role="tablist"></ul>');
    $(this).before(tabBar);

    // Add each code sample to the tab bar:
    var codeSamples = $(this).children("div");
    codeSamples.each(function(idx) {

      // The code becomes a tab-pane.
      $(this).addClass("tab-pane");
      $(this).attr("role", "tabpanel");

      var lang = $(this).data("lang");
      var image = $(this).data("image");
      var notabs = $(this).data("notabs");

      // Generating the labels
      var capitalizedLang = lang.substr(0, 1).toUpperCase() + lang.substr(1);
      var id = "tab_" + lang + "_" + counter;
      $(this).attr("id", id);
      var buttonLabel = "";
      if (image != null && langImages[lang]) {
        buttonLabel = "<img src='" +langImages[lang] + "' alt='" + capitalizedLang + "' />";
      } else if (notabs == null) {
        buttonLabel = "<b>" + capitalizedLang + "</b>";
      } else {
        buttonLabel = ""
      }

      // Add the link to the tab
      var active = "";
      if (idx == 0) {
        active = "active ";
        $(this).addClass("active");
      }

      tabBar.append(
        '<li class="nav-item"><button class="' +
        active + 'nav-link tab_' + lang + '" data-bs-target="#' +
        id + '" data-tab-lang="tab_' + lang + '" data-bs-toggle="tab">' +
        buttonLabel + '</button></li>'
      );
    });
    counter++;
  });
  $("ul.nav-tabs button").click(function (e) {
    // Toggling a tab should switch all tabs corresponding to the same language
    // while retaining the scroll position
    e.preventDefault();
    var scrollOffset = $(this).offset().top - $(document).scrollTop();
    $("." + $(this).attr('data-tab-lang')).tab('show');
    $(document).scrollTop($(this).offset().top - scrollOffset);
  });
}


$(function() {
  codeTabs();
  // Display anchor links when hovering over headers. For documentation of the
  // configuration options, see the AnchorJS documentation.
  anchors.options = {
    placement: 'right'
  };
  anchors.add();

  // Make dropdown menus in nav bars show on hover instead of click
  // using solution at https://webdesign.tutsplus.com/tutorials/how-
  // to-make-the-bootstrap-navbar-dropdown-work-on-hover--cms-33840
  const $dropdown = $(".dropdown");
  const $dropdownToggle = $(".dropdown-toggle");
  const $dropdownMenu = $(".dropdown-menu");
  const showClass = "show";
  $(window).on("load resize", function() {
    if (this.matchMedia("(min-width: 768px)").matches) {
      $dropdown.hover(
        function() {
          const $this = $(this);
          $this.addClass(showClass);
          $this.find($dropdownToggle).attr("aria-expanded", "true");
          $this.find($dropdownMenu).addClass(showClass);
        },
        function() {
          const $this = $(this);
          $this.removeClass(showClass);
          $this.find($dropdownToggle).attr("aria-expanded", "false");
          $this.find($dropdownMenu).removeClass(showClass);
        }
      );
    } else {
      $dropdown.off("mouseenter mouseleave");
    }
  });
});
