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

function drawApplicationTimeline(groupArray, eventObjArray) {
  var groups = new vis.DataSet(groupArray);
  var items = new vis.DataSet(eventObjArray);
  var container = $("#application-timeline")[0];
  var options = {
    groupOrder: function(a, b) {
      return a.value - b.value
    },
    editable: false,
    showCurrentTime: false,
    zoomable: false
  };

  var applicationTimeline = new vis.Timeline(container);
  applicationTimeline.setOptions(options);
  applicationTimeline.setGroups(groups);
  applicationTimeline.setItems(items);

  setupZoomable("#application-timeline-zoom-lock", applicationTimeline);

  $(".item.range.job.application-timeline-object").each(function() {
    $(this).click(function() {
      var jobIdText = $($(this).find(".application-timeline-content")[0]).text();
      var jobId = jobIdText.match("\\(Job (\\d+)\\)")[1];
      window.location.href = "#job-" + jobId;
    });
  });
}

function drawJobTimeline(groupArray, eventObjArray) {
  var groups = new vis.DataSet(groupArray);
  var items = new vis.DataSet(eventObjArray);
  var container = $('#job-timeline')[0];
  var options = {
    groupOrder: function(a, b) {
      return a.value - b.value;
    },
    editable: false,
    showCurrentTime: false,
    zoomable: false,
  };

  var jobTimeline = new vis.Timeline(container);
  jobTimeline.setOptions(options);
  jobTimeline.setGroups(groups);
  jobTimeline.setItems(items);

  setupZoomable("#job-timeline-zoom-lock", jobTimeline);

    $(".item.range.stage.job-timeline-object").each(function() {
      $(this).click(function() {
        var stageIdText = $($(this).find(".job-timeline-content")[0]).text();
        var stageId = stageIdText.match("\\(Stage (\\d+\\.\\d+)\\)")[1].replace(".", "-");
        window.location.href = "#stage-" + stageId;
      });
    });
}

function setupJobEntryLink(id, timeline) {
  $(id +'')
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
