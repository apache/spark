// © 2009–2010 EPFL/LAMP
// code by Gilles Dubochet with contributions by Johannes Rudolph and "spiros"

var topLevelTemplates = undefined;
var topLevelPackages = undefined;

var scheduler = undefined;

var kindFilterState = undefined;
var focusFilterState = undefined;

var title = $(document).attr('title');

var lastHash = "";

$(document).ready(function() {
    $('body').layout({
        west__size: '20%',
        center__maskContents: true
    });
    $('#browser').layout({
        center__paneSelector: ".ui-west-center"
        //,center__initClosed:true
        ,north__paneSelector: ".ui-west-north"
    });
    $('iframe').bind("load", function(){
        var subtitle = $(this).contents().find('title').text();
        $(document).attr('title', (title ? title + " - " : "") + subtitle);

        setUrlFragmentFromFrameSrc();
    });

    // workaround for IE's iframe sizing lack of smartness
    if($.browser.msie) {
        function fixIFrame() {
            $('iframe').height($(window).height() )
        }
        $('iframe').bind("load",fixIFrame)
        $('iframe').bind("resize",fixIFrame)
    }

    scheduler = new Scheduler();
    scheduler.addLabel("init", 1);
    scheduler.addLabel("focus", 2);
    scheduler.addLabel("filter", 4);

    prepareEntityList();

    configureTextFilter();
    configureKindFilter();
    configureEntityList();

    setFrameSrcFromUrlFragment();

    // If the url fragment changes, adjust the src of iframe "template".
    $(window).bind('hashchange', function() {
      if(lastFragment != window.location.hash) {
        lastFragment = window.location.hash;
        setFrameSrcFromUrlFragment();
      }
    });
});

// Set the iframe's src according to the fragment of the current url.
// fragment = "#scala.Either" => iframe url = "scala/Either.html"
// fragment = "#scala.Either@isRight:Boolean" => iframe url = "scala/Either.html#isRight:Boolean"
function setFrameSrcFromUrlFragment() {
  var fragment = location.hash.slice(1);
  if(fragment) {
    var loc = fragment.split("@")[0].replace(/\./g, "/");
    if(loc.indexOf(".html") < 0) loc += ".html";
    if(fragment.indexOf('@') > 0) loc += ("#" + fragment.split("@", 2)[1]);
    frames["template"].location.replace(loc);
  }
  else
    frames["template"].location.replace("package.html");
}

// Set the url fragment according to the src of the iframe "template".
// iframe url = "scala/Either.html"  =>  url fragment = "#scala.Either"
// iframe url = "scala/Either.html#isRight:Boolean"  =>  url fragment = "#scala.Either@isRight:Boolean"
function setUrlFragmentFromFrameSrc() {
  try {
    var commonLength = location.pathname.lastIndexOf("/");
    var frameLocation = frames["template"].location;
    var relativePath = frameLocation.pathname.slice(commonLength + 1);

    if(!relativePath || frameLocation.pathname.indexOf("/") < 0)
      return;

    // Add #, remove ".html" and replace "/" with "."
    fragment = "#" + relativePath.replace(/\.html$/, "").replace(/\//g, ".");

    // Add the frame's hash after an @
    if(frameLocation.hash) fragment += ("@" + frameLocation.hash.slice(1));

    // Use replace to not add history items
    lastFragment = fragment;
    location.replace(fragment);
  }
  catch(e) {
    // Chrome doesn't allow reading the iframe's location when
    // used on the local file system.
  }
}

var Index = {};

(function (ns) {
    function openLink(t, type) {
        var href;
        if (type == 'object') {
            href = t['object'];
        } else {
            href = t['class'] || t['trait'] || t['case class'] || t['type'];
        }
        return [
            '<a class="tplshow" target="template" href="',
            href,
            '"><img width="13" height="13" class="',
            type,
            ' icon" src="lib/',
            type,
            '.png" />'
        ].join('');
    }

    function createPackageHeader(pack) {
        return [
            '<li class="pack">',
            '<a class="packfocus">focus</a><a class="packhide">hide</a>',
            '<a class="tplshow" target="template" href="',
            pack.replace(/\./g, '/'),
            '/package.html">',
            pack,
            '</a></li>'
        ].join('');
    };

    function createListItem(template) {
        var inner = '';


        if (template.object) {
            inner += openLink(template, 'object');
        }

        if (template['class'] || template['trait'] || template['case class'] || template['type']) {
            inner += (inner == '') ?
                '<div class="placeholder" />' : '</a>';
            inner += openLink(template, template['trait'] ? 'trait' : template['type'] ? 'type' : 'class');
        } else {
            inner += '<div class="placeholder"/>';
        }

        return [
            '<li>',
            inner,
            '<span class="tplLink">',
            template.name.replace(/^.*\./, ''),
            '</span></a></li>'
        ].join('');
    }


    ns.createPackageTree = function (pack, matched, focused) {
        var html = $.map(matched, function (child, i) {
            return createListItem(child);
        }).join('');

        var header;
        if (focused && pack == focused) {
            header = '';
        } else {
            header = createPackageHeader(pack);
        }

        return [
            '<ol class="packages">',
            header,
            '<ol class="templates">',
            html,
            '</ol></ol>'
        ].join('');
    }

    ns.keys = function (obj) {
        var result = [];
        var key;
        for (key in obj) {
            result.push(key);
        }
        return result;
    }

    var hiddenPackages = {};

    function subPackages(pack) {
        return $.grep($('#tpl ol.packages'), function (element, index) {
            var pack = $('li.pack > .tplshow', element).text();
            return pack.indexOf(pack + '.') == 0;
        });
    }

    ns.hidePackage = function (ol) {
        var selected = $('li.pack > .tplshow', ol).text();
        hiddenPackages[selected] = true;

        $('ol.templates', ol).hide();

        $.each(subPackages(selected), function (index, element) {
            $(element).hide();
        });
    }

    ns.showPackage = function (ol, state) {
        var selected = $('li.pack > .tplshow', ol).text();
        hiddenPackages[selected] = false;

        $('ol.templates', ol).show();

        $.each(subPackages(selected), function (index, element) {
            $(element).show();

            // When the filter is in "packs" state,
            // we don't want to show the `.templates`
            var key = $('li.pack > .tplshow', element).text();
            if (hiddenPackages[key] || state == 'packs') {
                $('ol.templates', element).hide();
            }
        });
    }

})(Index);

function configureEntityList() {
    kindFilterSync();
    configureHideFilter();
    configureFocusFilter();
    textFilter();
}

/* Updates the list of entities (i.e. the content of the #tpl element) from the raw form generated by Scaladoc to a
   form suitable for display. In particular, it adds class and object etc. icons, and it configures links to open in
   the right frame. Furthermore, it sets the two reference top-level entities lists (topLevelTemplates and
   topLevelPackages) to serve as reference for resetting the list when needed.
   Be advised: this function should only be called once, on page load. */
function prepareEntityList() {
    var classIcon = $("#library > img.class");
    var traitIcon = $("#library > img.trait");
    var typeIcon = $("#library > img.type");
    var objectIcon = $("#library > img.object");
    var packageIcon = $("#library > img.package");

    $('#tpl li.pack > a.tplshow').attr("target", "template");
    $('#tpl li.pack').each(function () {
        $("span.class", this).each(function() { $(this).replaceWith(classIcon.clone()); });
        $("span.trait", this).each(function() { $(this).replaceWith(traitIcon.clone()); });
        $("span.type", this).each(function() { $(this).replaceWith(typeIcon.clone()); });
        $("span.object", this).each(function() { $(this).replaceWith(objectIcon.clone()); });
        $("span.package", this).each(function() { $(this).replaceWith(packageIcon.clone()); });
    });
    $('#tpl li.pack')
        .prepend("<a class='packhide'>hide</a>")
        .prepend("<a class='packfocus'>focus</a>");
}

/* Handles all key presses while scrolling around with keyboard shortcuts in left panel */
function keyboardScrolldownLeftPane() {
    scheduler.add("init", function() {
        $("#textfilter input").blur();
        var $items = $("#tpl li");
        $items.first().addClass('selected');

        $(window).bind("keydown", function(e) {
            var $old = $items.filter('.selected'),
                $new;

            switch ( e.keyCode ) {

            case 9: // tab
                $old.removeClass('selected');
                break;

            case 13: // enter
                $old.removeClass('selected');
                var $url = $old.children().filter('a:last').attr('href');
                $("#template").attr("src",$url);
                break;

            case 27: // escape
                $old.removeClass('selected');
                $(window).unbind(e);
                $("#textfilter input").focus();

                break;

            case 38: // up
                $new = $old.prev();

                if (!$new.length) {
                    $new = $old.parent().prev();
                }

                if ($new.is('ol') && $new.children(':last').is('ol')) {
                    $new = $new.children().children(':last');
                } else if ($new.is('ol')) {
                    $new = $new.children(':last');
                }

                break;

            case 40: // down
                $new = $old.next();
                if (!$new.length) {
                    $new = $old.parent().parent().next();
                }
                if ($new.is('ol')) {
                    $new = $new.children(':first');
                }
                break;
            }

            if ($new.is('li')) {
                $old.removeClass('selected');
                $new.addClass('selected');
            } else if (e.keyCode == 38) {
                $(window).unbind(e);
                $("#textfilter input").focus();
            }
        });
    });
}

/* Configures the text filter  */
function configureTextFilter() {
    scheduler.add("init", function() {
        $("#textfilter").append("<span class='pre'/><span class='input'><input id='index-input' type='text' accesskey='/'/></span><span class='post'/>");
        var input = $("#textfilter input");
        resizeFilterBlock();
        input.bind('keyup', function(event) {
            if (event.keyCode == 27) { // escape
                input.attr("value", "");
            }
            if (event.keyCode == 40) { // down arrow
                $(window).unbind("keydown");
                keyboardScrolldownLeftPane();
                return false;
            }
            textFilter();
        });
        input.bind('keydown', function(event) {
            if (event.keyCode == 9) { // tab
                $("#template").contents().find("#mbrsel-input").focus();
                input.attr("value", "");
                return false;
            }
            textFilter();
        });
        input.focus(function(event) { input.select(); });
    });
    scheduler.add("init", function() {
        $("#textfilter > .post").click(function(){
            $("#textfilter input").attr("value", "");
            textFilter();
        });
    });
}

function compilePattern(query) {
    var escaped = query.replace(/([\.\*\+\?\|\(\)\[\]\\])/g, '\\$1');

    if (query.toLowerCase() != query) {
        // Regexp that matches CamelCase subbits: "BiSe" is
        // "[a-z]*Bi[a-z]*Se" and matches "BitSet", "ABitSet", ...
        return new RegExp(escaped.replace(/([A-Z])/g,"[a-z]*$1"));
    }
    else { // if query is all lower case make a normal case insensitive search
        return new RegExp(escaped, "i");
    }
}

// Filters all focused templates and packages. This function should be made less-blocking.
//   @param query The string of the query
function textFilter() {
    scheduler.clear("filter");

    $('#tpl').html('');

    var query = $("#textfilter input").attr("value") || '';
    var queryRegExp = compilePattern(query);

    var index = 0;

    var searchLoop = function () {
        var packages = Index.keys(Index.PACKAGES).sort();

        while (packages[index]) {
            var pack = packages[index];
            var children = Index.PACKAGES[pack];
            index++;

            if (focusFilterState) {
                if (pack == focusFilterState ||
                    pack.indexOf(focusFilterState + '.') == 0) {
                    ;
                } else {
                    continue;
                }
            }

            var matched = $.grep(children, function (child, i) {
                return queryRegExp.test(child.name);
            });

            if (matched.length > 0) {
                $('#tpl').append(Index.createPackageTree(pack, matched,
                                                         focusFilterState));
                scheduler.add('filter', searchLoop);
                return;
            }
        }

        $('#tpl a.packfocus').click(function () {
            focusFilter($(this).parent().parent());
        });
        configureHideFilter();
    };

    scheduler.add('filter', searchLoop);
}

/* Configures the hide tool by adding the hide link to all packages. */
function configureHideFilter() {
    $('#tpl li.pack a.packhide').click(function () {
        var packhide = $(this)
        var action = packhide.text();

        var ol = $(this).parent().parent();

        if (action == "hide") {
            Index.hidePackage(ol);
            packhide.text("show");
        }
        else {
            Index.showPackage(ol, kindFilterState);
            packhide.text("hide");
        }
        return false;
    });
}

/* Configures the focus tool by adding the focus bar in the filter box (initially hidden), and by adding the focus
   link to all packages. */
function configureFocusFilter() {
    scheduler.add("init", function() {
        focusFilterState = null;
        if ($("#focusfilter").length == 0) {
            $("#filter").append("<div id='focusfilter'>focused on <span class='focuscoll'></span> <a class='focusremove'><img class='icon' src='lib/remove.png'/></a></div>");
            $("#focusfilter > .focusremove").click(function(event) {
                textFilter();

                $("#focusfilter").hide();
                $("#kindfilter").show();
                resizeFilterBlock();
                focusFilterState = null;
            });
            $("#focusfilter").hide();
            resizeFilterBlock();
        }
    });
    scheduler.add("init", function() {
        $('#tpl li.pack a.packfocus').click(function () {
            focusFilter($(this).parent());
            return false;
        });
    });
}

/* Focuses the entity index on a specific package. To do so, it will copy the sub-templates and sub-packages of the
   focuses package into the top-level templates and packages position of the index. The original top-level
     @param package The <li> element that corresponds to the package in the entity index */
function focusFilter(package) {
    scheduler.clear("filter");

    var currentFocus = $('li.pack > .tplshow', package).text();
    $("#focusfilter > .focuscoll").empty();
    $("#focusfilter > .focuscoll").append(currentFocus);

    $("#focusfilter").show();
    $("#kindfilter").hide();
    resizeFilterBlock();
    focusFilterState = currentFocus;
    kindFilterSync();

    textFilter();
}

function configureKindFilter() {
    scheduler.add("init", function() {
        kindFilterState = "all";
        $("#filter").append("<div id='kindfilter'><a>display packages only</a></div>");
        $("#kindfilter > a").click(function(event) { kindFilter("packs"); });
        resizeFilterBlock();
    });
}

function kindFilter(kind) {
    if (kind == "packs") {
        kindFilterState = "packs";
        kindFilterSync();
        $("#kindfilter > a").replaceWith("<a>display all entities</a>");
        $("#kindfilter > a").click(function(event) { kindFilter("all"); });
    }
    else {
        kindFilterState = "all";
        kindFilterSync();
        $("#kindfilter > a").replaceWith("<a>display packages only</a>");
        $("#kindfilter > a").click(function(event) { kindFilter("packs"); });
    }
}

/* Applies the kind filter. */
function kindFilterSync() {
    if (kindFilterState == "all" || focusFilterState != null) {
        $("#tpl a.packhide").text('hide');
        $("#tpl ol.templates").show();
    } else {
        $("#tpl a.packhide").text('show');
        $("#tpl ol.templates").hide();
    }
}

function resizeFilterBlock() {
    $("#tpl").css("top", $("#filter").outerHeight(true));
}
