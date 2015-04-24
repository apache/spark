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

function drawApplicationTimeline(groupArray, eventObjArray, startTime) {
  var groups = new vis.DataSet(groupArray);
  var items = new vis.DataSet(eventObjArray);
  var container = $("#application-timeline")[0];
  var options = {
    groupOrder: function(a, b) {
      return a.value - b.value
    },
    editable: false,
    showCurrentTime: false,
    min: startTime,
    zoomable: false
  };

  var applicationTimeline = new vis.Timeline(container);
  applicationTimeline.setOptions(options);
  applicationTimeline.setGroups(groups);
  applicationTimeline.setItems(items);

  setupZoomable("#application-timeline-zoom-lock", applicationTimeline);
  setupExecutorEventAction();

  function setupJobEventAction() {
    $(".item.range.job.application-timeline-object").each(function() {
      var getJobId = function(baseElem) {
        var jobIdText = $($(baseElem).find(".application-timeline-content")[0]).text();
        var jobId = jobIdText.match("\\(Job (\\d+)\\)")[1];
       return jobId;
      };

      $(this).click(function() {
        window.location.href = "job/?id=" + getJobId(this);
      });

      $(this).hover(
        function() {
          $("#job-" + getJobId(this)).addClass("corresponding-item-hover");
          $($(this).find("div.application-timeline-content")[0]).tooltip("show");
        },
        function() {
          $("#job-" + getJobId(this)).removeClass("corresponding-item-hover");
          $($(this).find("div.application-timeline-content")[0]).tooltip("hide");
        }
      );
    });
  }

  setupJobEventAction();

  $("span.expand-application-timeline").click(function() {
    $("#application-timeline").toggleClass('collapsed');

    // Switch the class of the arrow from open to closed.
    $(this).find('.expand-application-timeline-arrow').toggleClass('arrow-open');
    $(this).find('.expand-application-timeline-arrow').toggleClass('arrow-closed');
  });
}

function drawJobTimeline(groupArray, eventObjArray, startTime) {
  var groups = new vis.DataSet(groupArray);
  var items = new vis.DataSet(eventObjArray);
  var container = $('#job-timeline')[0];
  var options = {
    groupOrder: function(a, b) {
      return a.value - b.value;
    },
    editable: false,
    showCurrentTime: false,
    min: startTime,
    zoomable: false,
  };

  var jobTimeline = new vis.Timeline(container);
  jobTimeline.setOptions(options);
  jobTimeline.setGroups(groups);
  jobTimeline.setItems(items);

  setupZoomable("#job-timeline-zoom-lock", jobTimeline);
  setupExecutorEventAction();

  function setupStageEventAction() {
    $(".item.range.stage.job-timeline-object").each(function() {
      var getStageIdAndAttempt = function(baseElem) {
        var stageIdText = $($(baseElem).find(".job-timeline-content")[0]).text();
        var stageIdAndAttempt = stageIdText.match("\\(Stage (\\d+\\.\\d+)\\)")[1].split(".");
        return stageIdAndAttempt;
      };

      $(this).click(function() {
        var idAndAttempt = getStageIdAndAttempt(this);
        var id = idAndAttempt[0];
        var attempt = idAndAttempt[1];
        window.location.href = "../../stages/stage/?id=" + id + "&attempt=" + attempt;
      });

      $(this).hover(
        function() {
          var idAndAttempt = getStageIdAndAttempt(this);
          var id = idAndAttempt[0];
          var attempt = idAndAttempt[1];
          $("#stage-" + id + "-" + attempt).addClass("corresponding-item-hover");
          $($(this).find("div.job-timeline-content")[0]).tooltip("show");
        },
        function() {
          var idAndAttempt = getStageIdAndAttempt(this);
          var id = idAndAttempt[0];
          var attempt = idAndAttempt[1];
          $("#stage-" + id + "-" + attempt).removeClass("corresponding-item-hover");
          $($(this).find("div.job-timeline-content")[0]).tooltip("hide");
        }
      );
    });
  }

  setupStageEventAction();

  $("span.expand-job-timeline").click(function() {
    $("#job-timeline").toggleClass('collapsed');

    // Switch the class of the arrow from open to closed.
    $(this).find('.expand-job-timeline-arrow').toggleClass('arrow-open');
    $(this).find('.expand-job-timeline-arrow').toggleClass('arrow-closed');
  });
}

function setupExecutorEventAction() {
  $(".item.box.executor").each(function () {
    $(this).hover(
      function() {
        $($(this).find(".executor-event-content")[0]).tooltip("show");
      },
      function() {
        $($(this).find(".executor-event-content")[0]).tooltip("hide");
      }
    );
  });
}

function setupZoomable(id, timeline) {
  $(id + '>input[type="checkbox"]').click(function() {
    if (this.checked) {
      timeline.setOptions({zoomable: false});
    } else {
      timeline.setOptions({zoomable: true});
    }
  });

  $(id + ">span").click(function() {
    $(this).parent().find('input:checkbox').trigger('click');
  });
}
