/* memoize the formatted graph data */
var chart;
var svg;
var tableData;
var divisorAndTitle;
var numTasks;
var divisor;
var xTitle;

function renderJobsGraphs(data) {
  /* show visualization toggle */
  $(".expand-visualization-arrow").toggleClass('arrow-closed');
  $(".expand-visualization-arrow").toggleClass('arrow-open');
  if ($(".expand-visualization-arrow").hasClass("arrow-closed")) {
    $("#chartContainer").empty();
    return;
  }

  /* no data to graph */
  if (!Object.keys(data).length) {
    return;
  }

  /* format data for dimple.js */
  if (!tableData) {
    tableData = [];
    var startTime = getMin(data["Launch Time"]);
    numTasks = Math.min(1000, data["Launch Time"].length);

    /*data update */
    data["Launch Time"] = data["Launch Time"].map(function(launchTime) {
      return launchTime - startTime;
    });
    var maxTime = 0;
    for (i = 0; i < numTasks; i++) {
      var time = 0;
      for (var key in data) {
        time += data[key][i];
      }
      maxTime = Math.max(time, maxTime);
    }
    setDivisiorAndTitle(maxTime);
    for (i = 0; i < numTasks; i++) {
      for (var key in data) {
        job = {};
        job["Task #"] = i;
        job["Task"] = key;
        job["Time"] = data[key][i] / divisor;
        tableData.push(job);
      }
    }
  }

  var height = Math.max(Math.min(numTasks * 50, 2000), 200);
  svg = dimple.newSvg("#chartContainer", "100%", height);
  chart = new dimple.chart(svg);
  chart.setMargins(60, 80, 60, 60);

  var x = chart.addMeasureAxis("x", "Time");
  x.fontSize = "12px";
  x.title = xTitle;

  var y = chart.addCategoryAxis("y", "Task #");
  y.fontSize = "12px";

  var s = chart.addSeries("Task", dimple.plot.bar);
  s.data = tableData;
  s.addOrderRule(getOrderRule());

  chart.addLegend(20, 10, "80%", 60, "left");
  (chart.legends[0]).fontSize = "12px";

  s.getTooltipText = function(dat) {
    return ["Task #: " + dat["yField"][0],
      "Phase: " + dat["aggField"][0],
      "Time (ms): " + dat["xValue"] * divisor
    ];
  };

  chart.draw(100);
  svg.selectAll(".dimple-launch-time").remove();
  numTicks(y, Math.floor(numTasks / 20));
}

function getMin(arr) {
  return Math.min.apply(null, arr);
}

function getOrderRule() {
  return ["Launch Time", "Scheduler Delay", "Task Deserialization Time",
    "Duration", "Result Serialization Time", "Getting Result Time", "GC Time"
  ];
}

function setDivisiorAndTitle(maxTime) {
  var sec = 1000;
  var min = sec * 60;
  var hr = min * 60;
  if (maxTime >= hr) {
    divisor = hr;
    xTitle = "Time (hr)";
  } else if (maxTime >= min) {
    divisor = min;
    xTitle = "Time (min)";
  } else if (maxTime >= sec) {
    divisor = sec;
    xTitle = "Time (s)";
  } else {
    divisor = 1;
    xTitle = "Time (ms)";
  }
}

/* limits the number of ticks in the Y-axis to oneInEvery */
function numTicks(axis, oneInEvery) {
  if (axis.shapes.length > 0) {
    var del = 0;
    if (oneInEvery > 1) {
      axis.shapes.selectAll("text").each(function(d) {
        if (del % oneInEvery !== 0) {
          this.remove();
          axis.shapes.selectAll("line").each(function(d2) {
            if (d === d2) {
              this.remove();
            }
          });
        }
        del += 1;
      });
    }
  }
}

window.onresize = function() {
  if ($(".expand-visualization-arrow").hasClass("arrow-open")) {
    chart.draw(0, true);
  }
};