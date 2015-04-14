function renderJobsGraphs(data) {
	console.log(JSON.stringify(data));
	var svg = dimple.newSvg("#chartContainer", 590, 400);
	var myChart = new dimple.chart(svg, data);
	myChart.setBounds(75, 30, 480, 330);
	myChart.addMeasureAxis("x", "Time");
	myChart.addCategoryAxis("y", "Job#");
	myChart.addSeries("Scheduler Delays", dimple.plot.bar);
	myChart.addLegend(60, 10, 510, 20, "right");
	myChart.draw();
}