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

	/* format data to a form readable by dimple.js */
	var tableData = [];
	for (var k in data) {
		var arr = (data[k]).split(",");
		data[k] = arr;
	}
	var startTime = getMin(data["launchtime"]);
	var numTasks = Math.min(1000, data[k].length);

	/*data update */
	data["launchtime"] = data["launchtime"].map(function (launchTime) {return launchTime-startTime;});
	var maxTime = 0;
	for (i = 0; i < numTasks; i++) {
		var time = 0;
		for (var key in data) {
			time += parseFloat(data[key][i]);
		}
		maxTime = Math.max(time, maxTime);
	}
	divisorAndTitle = getDivisiorAndTitle(maxTime);
	for (i = 0; i < numTasks; i++) {
		for (var key in data) {
			job = {};
			job["Task #"] = i;
			job["Task"] = key;
			job["Time"] = parseFloat(data[key][i])/divisorAndTitle[0];
			tableData.push(job);
		}
	}

	var height = Math.max(Math.min(numTasks * 50, 2000), 200);
	var svg = dimple.newSvg("#chartContainer", "100%", height);
	var chart = new dimple.chart(svg);
	chart.setMargins(60, 60, 60, 60);

	var x = chart.addMeasureAxis("x", "Time");
	x.fontSize = "12px";
	x.title = divisorAndTitle[1];

	var y = chart.addCategoryAxis("y", "Task #");
	y.fontSize = "12px";

	var s = chart.addSeries("Task", dimple.plot.bar);
	s.data = tableData;
	s.addOrderRule(getOrderRule());

	s.getTooltipText = function (dat) {
		return ["Task #: " + dat["yField"][0], 
						"Phase: " +  dat["aggField"][0], 
						"Time (ms): " + dat["xValue"]*divisorAndTitle[0]
						];
	};

	chart.addLegend(20, 10, 1000, 40, "right");
	(chart.legends[0]).fontSize = "12px";

	chart.draw();
	svg.selectAll(".dimple-launchtime").remove();
	numTicks(y, Math.floor(numTasks/20));
}

function getMin(arr) {
  return Math.min.apply(null, arr);
}

function getOrderRule() {
	return ["launchtime", "Scheduler Delay", "Task Deserialization Time", 
					"Duration", "Result Serialization Time", "Getting Result Time", "GC Time"];
}

function getDivisiorAndTitle(maxTime) {
	var sec = 1000;
	var min = sec * 60;
	var hr = min * 60;
	if (maxTime >= hr) {
		return [hr, "Time (hr)"];
	} else if (maxTime >= min) {
		return [min, "Time (min)"];
	} else if (maxTime >= sec) {
		return [sec, "Time (s)"];
	} else {
		return [1, "Time (ms)"];
	}
}

/* limits the number of ticks in the Y-axis to oneInEvery */
function numTicks(axis, oneInEvery) {
	if (axis.shapes.length > 0) {
		var del = 0;
		if (oneInEvery > 1) {
			axis.shapes.selectAll("text").each(function (d) {
				if (del % oneInEvery !== 0) {
					this.remove();
					axis.shapes.selectAll("line").each(function (d2) {
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