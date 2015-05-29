/**
 * JavaScript functions enhancing the SVG diagrams.
 * 
 * @author Damien Obrist
 */

var diagrams = {};

/**
 * Initializes the diagrams in the main window.
 */
$(document).ready(function()
{
	// hide diagrams in browsers not supporting SVG
	if(Modernizr && !Modernizr.inlinesvg)
		return;

	// only execute this in the main window
	if(diagrams.isPopup)
		return;

	if($("#content-diagram").length)
		$("#inheritance-diagram").css("padding-bottom", "20px");

	$(".diagram-container").css("display", "block");

	$(".diagram").each(function() {
		// store inital dimensions
		$(this).data("width", $("svg", $(this)).width());
		$(this).data("height", $("svg", $(this)).height());
		// store unscaled clone of SVG element
		$(this).data("svg", $(this).get(0).childNodes[0].cloneNode(true));
	});
	
	// make diagram visible, hide container
	$(".diagram").css("display", "none");
	$(".diagram svg").css({
		"position": "static",
		"visibility": "visible",
		"z-index": "auto"
	});

	// enable linking to diagrams
	if($(location).attr("hash") == "#inheritance-diagram") {
		diagrams.toggle($("#inheritance-diagram-container"), true);
	} else if($(location).attr("hash") == "#content-diagram") {
		diagrams.toggle($("#content-diagram-container"), true);
	}

	$(".diagram-link").click(function() {
		diagrams.toggle($(this).parent());
	});

	// register resize function
	$(window).resize(diagrams.resize);

	// don't bubble event to parent div
	// when clicking on a node of a resized
	// diagram
	$("svg a").click(function(e) {
		e.stopPropagation();
	});

	diagrams.initHighlighting();
});

/**
 * Initializes the diagrams in the popup.
 */
diagrams.initPopup = function(id)
{
	// copy diagram from main window
	if(!jQuery.browser.msie)
		$("body").append(opener.$("#" + id).data("svg"));

	// positioning
	$("svg").css("position", "absolute");
	$(window).resize(function()
	{
		var svg_w = $("svg").css("width").replace("px", "");
		var svg_h = $("svg").css("height").replace("px", "");
		var x = $(window).width() / 2 - svg_w / 2;
		if(x < 0) x = 0;
		var y = $(window).height() / 2 - svg_h / 2;
		if(y < 0) y = 0;
		$("svg").css("left", x + "px");
		$("svg").css("top", y + "px");
	});
	$(window).resize();

	diagrams.initHighlighting();
	$("svg a").click(function(e) {
		opener.diagrams.redirectFromPopup(this.href.baseVal);
		window.close();
	});
	$(document).keyup(function(e) {
		if (e.keyCode == 27) window.close();
	});
}

/**
 * Initializes highlighting for nodes and edges.
 */
diagrams.initHighlighting = function()
{
	// helper function since $.hover doesn't work in IE

	function hover(elements, fn)
	{
		elements.mouseover(fn);
		elements.mouseout(fn);
	}

	// inheritance edges

	hover($("svg .edge.inheritance"), function(evt){
		var toggleClass = evt.type == "mouseout" ? diagrams.removeClass : diagrams.addClass;
		var parts = $(this).attr("id").split("_");
		toggleClass($("#" + parts[0] + "_" + parts[1]));
		toggleClass($("#" + parts[0] + "_" + parts[2]));
		toggleClass($(this));
	});

	// nodes

	hover($("svg .node"), function(evt){
		var toggleClass = evt.type == "mouseout" ? diagrams.removeClass : diagrams.addClass;
		toggleClass($(this));
		var parts = $(this).attr("id").split("_");
		var index = parts[1];
		$("svg#" + parts[0] + " .edge.inheritance").each(function(){
			var parts2 = $(this).attr("id").split("_");
			if(parts2[1] == index)
			{
				toggleClass($("#" + parts2[0] + "_" + parts2[2]));
				toggleClass($(this));
			} else if(parts2[2] == index)
			{
				toggleClass($("#" + parts2[0] + "_" + parts2[1]));
				toggleClass($(this));
			}
		});
	});

	// incoming implicits

	hover($("svg .node.implicit-incoming"), function(evt){
		var toggleClass = evt.type == "mouseout" ? diagrams.removeClass : diagrams.addClass;
		toggleClass($(this));
		toggleClass($("svg .edge.implicit-incoming"));
		toggleClass($("svg .node.this"));
	});

	hover($("svg .edge.implicit-incoming"), function(evt){
		var toggleClass = evt.type == "mouseout" ? diagrams.removeClass : diagrams.addClass;
		toggleClass($(this));
		toggleClass($("svg .node.this"));
		$("svg .node.implicit-incoming").each(function(){
			toggleClass($(this));
		});
	});
	
	// implicit outgoing nodes

	hover($("svg .node.implicit-outgoing"), function(evt){
		var toggleClass = evt.type == "mouseout" ? diagrams.removeClass : diagrams.addClass;
		toggleClass($(this));
		toggleClass($("svg .edge.implicit-outgoing"));
		toggleClass($("svg .node.this"));
	});

	hover($("svg .edge.implicit-outgoing"), function(evt){
		var toggleClass = evt.type == "mouseout" ? diagrams.removeClass : diagrams.addClass;
		toggleClass($(this));
		toggleClass($("svg .node.this"));
		$("svg .node.implicit-outgoing").each(function(){
			toggleClass($(this));
		});
	});
};

/**
 * Resizes the diagrams according to the available width.
 */
diagrams.resize = function()
{
	// available width
	var availableWidth = $("body").width() - 20;

	$(".diagram-container").each(function() {
		// unregister click event on whole div
		$(".diagram", this).unbind("click");
		var diagramWidth = $(".diagram", this).data("width");
		var diagramHeight = $(".diagram", this).data("height");

		if(diagramWidth > availableWidth)
		{
			// resize diagram
			var height = diagramHeight / diagramWidth * availableWidth;
			$(".diagram svg", this).width(availableWidth);
			$(".diagram svg", this).height(height);

			// register click event on whole div
			$(".diagram", this).click(function() {
				diagrams.popup($(this));
			});
			$(".diagram", this).addClass("magnifying");
		}
		else
		{
			// restore full size of diagram
			$(".diagram svg", this).width(diagramWidth);
			$(".diagram svg", this).height(diagramHeight);
			// don't show custom cursor any more
			$(".diagram", this).removeClass("magnifying");
		}
	});
};

/**
 * Shows or hides a diagram depending on its current state.
 */
diagrams.toggle = function(container, dontAnimate)
{
	// change class of link
	$(".diagram-link", container).toggleClass("open");
	// get element to show / hide
	var div = $(".diagram", container);
	if (div.is(':visible'))
	{
		$(".diagram-help", container).hide();
		div.unbind("click");
		div.removeClass("magnifying");
		div.slideUp(100);
	}
	else
	{
		diagrams.resize();
		if(dontAnimate)
			div.show();
		else
			div.slideDown(100);
		$(".diagram-help", container).show();
	}
};

/**
 * Opens a popup containing a copy of a diagram.
 */
diagrams.windows = {};
diagrams.popup = function(diagram)
{
	var id = diagram.attr("id");
	if(!diagrams.windows[id] || diagrams.windows[id].closed) {
		var title = $(".symbol .name", $("#signature")).text();
		// cloning from parent window to popup somehow doesn't work in IE
		// therefore include the SVG as a string into the HTML
		var svgIE = jQuery.browser.msie ? $("<div />").append(diagram.data("svg")).html() : "";
		var html = '' +
		'<?xml version="1.0" encoding="UTF-8"?>\n' +
		'<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.1//EN" "http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd">\n' + 
		'<html>\n' +
		'	<head>\n' +
		'		<title>' + title + '</title>\n' +
		'		<link href="' + $("#diagrams-css").attr("href") + '" media="screen" type="text/css" rel="stylesheet" />\n' +
		'		<script type="text/javascript" src="' + $("#jquery-js").attr("src") + '"></script>\n' +
		'		<script type="text/javascript" src="' + $("#diagrams-js").attr("src") + '"></script>\n' +
		'		<script type="text/javascript">\n' +
		'			diagrams.isPopup = true;\n' +
		'		</script>\n' +
		'	</head>\n' +
		'	<body onload="diagrams.initPopup(\'' + id + '\');">\n' +
		'		<a href="#" onclick="window.close();" id="close-link">Close this window</a>\n' +
		'		' + svgIE + '\n' +
		'	</body>\n' +
		'</html>';

		var padding = 30;
		var screenHeight = screen.availHeight;
		var screenWidth = screen.availWidth;
		var w = Math.min(screenWidth, diagram.data("width") + 2 * padding);
		var h = Math.min(screenHeight, diagram.data("height") + 2 * padding);
		var left = (screenWidth - w) / 2;
		var top = (screenHeight - h) / 2;
		var parameters = "height=" + h + ", width=" + w + ", left=" + left + ", top=" + top + ", scrollbars=yes, location=no, resizable=yes";
		var win = window.open("about:blank", "_blank", parameters);
		win.document.open();
		win.document.write(html);
		win.document.close();
		diagrams.windows[id] = win;
	}
	win.focus();
};

/**
 * This method is called from within the popup when a node is clicked.
 */
diagrams.redirectFromPopup = function(url)
{
	window.location = url;
};

/**
 * Helper method that adds a class to a SVG element.
 */
diagrams.addClass = function(svgElem, newClass) {
	newClass = newClass || "over";
	var classes = svgElem.attr("class");
	if ($.inArray(newClass, classes.split(/\s+/)) == -1) {
		classes += (classes ? ' ' : '') + newClass;
		svgElem.attr("class", classes);
	}
};

/**
 * Helper method that removes a class from a SVG element.
 */
diagrams.removeClass = function(svgElem, oldClass) {
	oldClass = oldClass || "over";
	var classes = svgElem.attr("class");
	classes = $.grep(classes.split(/\s+/), function(n, i) { return n != oldClass; }).join(' ');
	svgElem.attr("class", classes);
};

