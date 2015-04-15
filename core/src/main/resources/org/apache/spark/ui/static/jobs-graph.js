function renderJobsGraphs(data) {
	var tableData = [];
	for (var k in data) {
		var arr = (data[k]).split(",");
		data[k] = arr;
	}
	var startTime = getMin(data["Launch Time"]);
	var numTasks = data[k].length;
	var maxTime = 0;
	console.log(numTasks);
	data["Launch Time"] = data["Launch Time"].map(function (launchTime) {return launchTime-startTime;});
	for (i = 0; i < data[k].length; i++) {
		var time = 0;
		for (var key in data) {
			job = {};
			job["Task #"] = i+1;
			job["Task"] = key;
			job["Time"] = parseFloat(data[key][i]);
			time += job["Time"];
			tableData.push(job);
		}
		maxTime = Math.max(time, maxTime);
	}
	console.log(maxTime);
	var height = Math.min(numTasks * 100, 2000);
	var width = Math.min(maxTime * 2.5, 2000);
	var svg = dimple.newSvg("#chartContainer", "100%", height);
	var chart = new dimple.chart(svg);
	chart.setBounds("5%","20%","90%","60%");
	var x = chart.addMeasureAxis("x", "Time");
	var y = chart.addCategoryAxis("y", "Task #");
	x.title = "Time (ms)";
	x.fontSize = "12px";
	y.fontSize = "12px";
	var s = chart.addSeries("Task", dimple.plot.bar);
	s.data = tableData;
	s.addOrderRule(["Launch Time", "Scheduler Delay", "Task Deserialization Time", "Duration", "Result Serialization Time", "Getting Result Time", "GC Time"]);
	chart.addLegend("20%", "10%", "80%", "20%", "right");
	(chart.legends[0]).fontSize = "12px";
	chart.draw();
}

function getMin(arr) {
  return Math.min.apply(null, arr);
}