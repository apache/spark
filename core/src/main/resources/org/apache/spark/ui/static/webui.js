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

/* eslint-disable no-unused-vars */
/**
 * Show a Bootstrap 5 Toast notification (non-blocking replacement for alert()).
 * @param {string} message - The message to display
 * @param {string} [type="danger"] - Bootstrap color: "danger", "warning", "success", "info"
 */
function showToast(message, type) {
  type = type || "danger";
  var container = document.getElementById("spark-toast-container");
  if (!container) {
    container = document.createElement("div");
    container.id = "spark-toast-container";
    container.className = "toast-container position-fixed top-0 end-0 p-3";
    container.style.zIndex = "1090";
    document.body.appendChild(container);
  }
  var toastEl = document.createElement("div");
  toastEl.className = "toast align-items-center text-bg-" + type + " border-0";
  toastEl.setAttribute("role", "alert");
  toastEl.innerHTML =
    '<div class="d-flex">' +
    '<div class="toast-body">' + message + '</div>' +
    '<button type="button" class="btn-close btn-close-white me-2 m-auto" ' +
    'data-bs-dismiss="toast"></button></div>';
  container.appendChild(toastEl);
  var toast = new bootstrap.Toast(toastEl, { autohide: false });
  toast.show();
  toastEl.addEventListener("hidden.bs.toast", function () {
    toastEl.remove();
  });
}
/* eslint-enable no-unused-vars */

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
