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

/* global $, loadMore, loadNew, toggleDagViz, togglePlanViz, clickPhysicalPlanDetails */
/* eslint-disable no-unused-vars */
var uiRoot = "";
var appBasePath = "";

function setUIRoot(val) {
  uiRoot = val;
}

function setAppBasePath(path) {
  appBasePath = path;
}
/* eslint-enable no-unused-vars */

// Theme toggle (dark/light mode)
function updateThemeButton(btn, theme) {
  btn.textContent = "\u25D1";
  btn.setAttribute("title", theme === "dark" ? "Switch to light mode" : "Switch to dark mode");
  // Swap logo for dark/light mode
  document.querySelectorAll("img.spark-logo").forEach(function(img) {
    var src = img.getAttribute("src") || "";
    if (theme === "dark") {
      img.setAttribute("src", src.replace("spark-logo.svg", "spark-logo-rev.svg"));
    } else {
      img.setAttribute("src", src.replace("spark-logo-rev.svg", "spark-logo.svg"));
    }
  });
}

function initThemeToggle() {
  var saved = localStorage.getItem("spark-theme");
  var theme = saved ||
    (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
  document.documentElement.setAttribute("data-bs-theme", theme);

  var btn = document.getElementById("theme-toggle");
  if (btn) {
    updateThemeButton(btn, theme);
    btn.addEventListener("click", function() {
      var current = document.documentElement.getAttribute("data-bs-theme");
      var next = current === "dark" ? "light" : "dark";
      document.documentElement.setAttribute("data-bs-theme", next);
      localStorage.setItem("spark-theme", next);
      updateThemeButton(btn, next);
    });
  }
}

// Initialize theme toggle
$(function() {
  initThemeToggle();
});

// Persist BS5 collapse state in localStorage
$(function() {
  $(document).on("shown.bs.collapse hidden.bs.collapse", ".collapsible-table", function(e) {
    var trigger = document.querySelector('[data-bs-target="#' + this.id + '"]');
    if (trigger) {
      var name = trigger.getAttribute("data-collapse-name");
      if (name) {
        var isShown = e.type === "shown";
        window.localStorage.setItem(name, "" + !isShown);
        $(trigger).find('.collapse-table-arrow')
          .toggleClass('arrow-open', isShown)
          .toggleClass('arrow-closed', !isShown);
        trigger.setAttribute("aria-expanded", "" + isShown);
      }
    }
  });

  // Restore collapse state from localStorage on page load
  $("[data-collapse-name]").each(function() {
    var name = this.getAttribute("data-collapse-name");
    var target = this.getAttribute("data-bs-target");
    if (name && target && window.localStorage.getItem(name) === "true") {
      var contentEl = document.querySelector(target);
      if (contentEl) {
        $(contentEl).removeClass("show");
        $(this).find('.collapse-table-arrow')
          .removeClass('arrow-open').addClass('arrow-closed');
        this.setAttribute("aria-expanded", "false");
      }
    }
  });
});

$(function() {
  // Show/hide full job description on click event.
  $(".description-input").click(function() {
    $(this).toggleClass("description-input-full");
  });
});

// Event delegation for CSP-compliant inline event handler replacement.
$(function() {
  // toggle details (stage-details, stacktrace-details, expand-details)
  $(document).on("click", "[data-toggle-details]", function() {
    var selector = $(this).data("toggle-details");
    this.parentNode.querySelector(selector).classList.toggle("collapsed");
  });

  // toggle sub-execution list (tr two siblings away from parent tr)
  $(document).on("click", "[data-toggle-sub-execution]", function() {
    $(this).closest("tr").nextAll("tr.sub-execution-list").first().toggleClass("collapsed");
  });

  // kill links with confirmation
  $(document).on("click", "a.kill-link[data-kill-message]", function(e) {
    if (!window.confirm($(this).data("kill-message"))) {
      e.preventDefault();
    } else if ($(this).closest("form").length > 0) {
      e.preventDefault();
      $(this).closest("form").submit();
    }
  });

  // loadMore / loadNew buttons
  $(document).on("click", ".log-more-btn", function() { loadMore(); });
  $(document).on("click", ".log-new-btn", function() { loadNew(); });

  // toggleDagViz
  $(document).on("click", ".expand-dag-viz[data-forjob]", function() {
    toggleDagViz($(this).data("forjob"));
  });

  // togglePlanViz / clickPhysicalPlanDetails
  $(document).on("click", "[data-action=togglePlanViz]", function() { togglePlanViz(); });
  $(document).on("click", "[data-action=clickPhysicalPlanDetails]", function() {
    clickPhysicalPlanDetails();
  });
});

// Footer uptime counter
$(function() {
  var el = document.getElementById("footer-uptime");
  if (!el) return;
  function update() {
    var ms = Date.now() - Number(el.dataset.startTime);
    var m = Math.floor(ms / 60000), h = Math.floor(m / 60), d = Math.floor(h / 24);
    el.textContent = "Uptime: " + (d > 0 ? d + "d " : "") + (h % 24) + "h " + (m % 60) + "m";
  }
  update();
  setInterval(update, 60000);
});
