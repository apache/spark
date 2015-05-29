// © 2009–2010 EPFL/LAMP
// code by Gilles Dubochet with contributions by Pedro Furlanetto

$(document).ready(function(){

    // Escapes special characters and returns a valid jQuery selector
    function escapeJquery(str){
        return str.replace(/([;&,\.\+\*\~':"\!\^#$%@\[\]\(\)=>\|])/g, '\\$1');
    }

    // highlight and jump to selected member
    if (window.location.hash) {
      var temp = window.location.hash.replace('#', '');
      var elem = '#'+escapeJquery(temp);

      window.scrollTo(0, 0);
      $(elem).parent().effect("highlight", {color: "#FFCC85"}, 3000);
      $('html,body').animate({scrollTop:$(elem).parent().offset().top}, 1000);
    }
    
    var isHiddenClass = function (name) {
        return name == 'scala.Any' ||
               name == 'scala.AnyRef';
    };

    var isHidden = function (elem) {
        return $(elem).attr("data-hidden") == 'true';
    };

    $("#linearization li:gt(0)").filter(function(){
        return isHiddenClass($(this).attr("name"));
    }).removeClass("in").addClass("out");

    $("#implicits li").filter(function(){
        return isHidden(this);
    }).removeClass("in").addClass("out");

    // Pre-filter members
    filter();

    // Member filter box
    var input = $("#textfilter input");
    input.bind("keyup", function(event) {

        switch ( event.keyCode ) {

        case 27: // escape key
            input.val("");
            filter(true);
            break;

        case 38: // up
            input.val("");
            filter(false);
            window.scrollTo(0, $("body").offset().top);
            input.focus();
            break;

        case 33: //page up
            input.val("");
            filter(false);
            break;

        case 34: //page down
            input.val("");
            filter(false);
            break;

        default:
            window.scrollTo(0, $("#mbrsel").offset().top);
            filter(true);
            break;

        }
    });
    input.focus(function(event) {
        input.select();
    });
    $("#textfilter > .post").click(function() {
        $("#textfilter input").attr("value", "");
        filter();
    });
    $(document).keydown(function(event) {

        if (event.keyCode == 9) { // tab
            $("#index-input", window.parent.document).focus();
            input.attr("value", "");
            return false;
        }
    });

    $("#linearization li").click(function(){
        if ($(this).hasClass("in")) {
            $(this).removeClass("in");
            $(this).addClass("out");
        }
        else if ($(this).hasClass("out")) {
            $(this).removeClass("out");
            $(this).addClass("in");
        };
        filter();
    });

    $("#implicits li").click(function(){
        if ($(this).hasClass("in")) {
            $(this).removeClass("in");
            $(this).addClass("out");
        }
        else if ($(this).hasClass("out")) {
            $(this).removeClass("out");
            $(this).addClass("in");
        };
        filter();
    });

    $("#mbrsel > div[id=ancestors] > ol > li.hideall").click(function() {
        $("#linearization li.in").removeClass("in").addClass("out");
        $("#linearization li:first").removeClass("out").addClass("in");
        $("#implicits li.in").removeClass("in").addClass("out");

        if ($(this).hasClass("out") && $("#mbrsel > div[id=ancestors] > ol > li.showall").hasClass("in")) {
            $(this).removeClass("out").addClass("in");
            $("#mbrsel > div[id=ancestors] > ol > li.showall").removeClass("in").addClass("out");
        }

        filter();
    })
    $("#mbrsel > div[id=ancestors] > ol > li.showall").click(function() {
        var filteredLinearization =
            $("#linearization li.out").filter(function() {
                return ! isHiddenClass($(this).attr("name"));
            });
        filteredLinearization.removeClass("out").addClass("in");

        var filteredImplicits =
        $("#implicits li.out").filter(function() {
            return ! isHidden(this);
        });
        filteredImplicits.removeClass("out").addClass("in");

        if ($(this).hasClass("out") && $("#mbrsel > div[id=ancestors] > ol > li.hideall").hasClass("in")) {
            $(this).removeClass("out").addClass("in");
            $("#mbrsel > div[id=ancestors] > ol > li.hideall").removeClass("in").addClass("out");
        }

        filter();
    });
    $("#visbl > ol > li.public").click(function() {
        if ($(this).hasClass("out")) {
            $(this).removeClass("out").addClass("in");
            $("#visbl > ol > li.all").removeClass("in").addClass("out");
            filter();
        };
    })
    $("#visbl > ol > li.all").click(function() {
        if ($(this).hasClass("out")) {
            $(this).removeClass("out").addClass("in");
            $("#visbl > ol > li.public").removeClass("in").addClass("out");
            filter();
        };
    });
    $("#order > ol > li.alpha").click(function() {
        if ($(this).hasClass("out")) {
            orderAlpha();
        };
    })
    $("#order > ol > li.inherit").click(function() {
        if ($(this).hasClass("out")) {
            orderInherit();
        };
    });
    $("#order > ol > li.group").click(function() {
        if ($(this).hasClass("out")) {
            orderGroup();
        };
    });
    $("#groupedMembers").hide();

    initInherit();

    // Create tooltips
    $(".extype").add(".defval").tooltip({
        tip: "#tooltip",
        position:"top center",
        predelay: 500,
        onBeforeShow: function(ev) {
            $(this.getTip()).text(this.getTrigger().attr("name"));
        }
    });

    /* Add toggle arrows */
    //var docAllSigs = $("#template li").has(".fullcomment").find(".signature");
    // trying to speed things up a little bit
    var docAllSigs = $("#template li[fullComment=yes] .signature");

    function commentToggleFct(signature){
        var parent = signature.parent();
        var shortComment = $(".shortcomment", parent);
        var fullComment = $(".fullcomment", parent);
        var vis = $(":visible", fullComment);
        signature.toggleClass("closed").toggleClass("opened");
        if (vis.length > 0) {
            shortComment.slideDown(100);
            fullComment.slideUp(100);
        }
        else {
            shortComment.slideUp(100);
            fullComment.slideDown(100);
        }
    };
    docAllSigs.addClass("closed");
    docAllSigs.click(function() {
        commentToggleFct($(this));
    });

    /* Linear super types and known subclasses */
    function toggleShowContentFct(e){
      e.toggleClass("open");
      var content = $(".hiddenContent", e.parent().get(0));
      if (content.is(':visible')) {
        content.slideUp(100);
      }
      else {
        content.slideDown(100);
      }
    };

    $(".toggle:not(.diagram-link)").click(function() {
      toggleShowContentFct($(this));
    });

    // Set parent window title
    windowTitle();

    if ($("#order > ol > li.group").length == 1) { orderGroup(); };
});

function orderAlpha() {
    $("#order > ol > li.alpha").removeClass("out").addClass("in");
    $("#order > ol > li.inherit").removeClass("in").addClass("out");
    $("#order > ol > li.group").removeClass("in").addClass("out");
    $("#template > div.parent").hide();
    $("#template > div.conversion").hide();
    $("#mbrsel > div[id=ancestors]").show();
    filter();
};

function orderInherit() {
    $("#order > ol > li.inherit").removeClass("out").addClass("in");
    $("#order > ol > li.alpha").removeClass("in").addClass("out");
    $("#order > ol > li.group").removeClass("in").addClass("out");
    $("#template > div.parent").show();
    $("#template > div.conversion").show();
    $("#mbrsel > div[id=ancestors]").hide();
    filter();
};

function orderGroup() {
    $("#order > ol > li.group").removeClass("out").addClass("in");
    $("#order > ol > li.alpha").removeClass("in").addClass("out");
    $("#order > ol > li.inherit").removeClass("in").addClass("out");
    $("#template > div.parent").hide();
    $("#template > div.conversion").hide();
    $("#mbrsel > div[id=ancestors]").show();
    filter();
};

/** Prepares the DOM for inheritance-based display. To do so it will:
  *  - hide all statically-generated parents headings;
  *  - copy all members from the value and type members lists (flat members) to corresponding lists nested below the
  *    parent headings (inheritance-grouped members);
  *  - initialises a control variable used by the filter method to control whether filtering happens on flat members
  *    or on inheritance-grouped members. */
function initInherit() {
    // inheritParents is a map from fully-qualified names to the DOM node of parent headings.
    var inheritParents = new Object();
    var groupParents = new Object();
    $("#inheritedMembers > div.parent").each(function(){
        inheritParents[$(this).attr("name")] = $(this);
    });
    $("#inheritedMembers > div.conversion").each(function(){
        inheritParents[$(this).attr("name")] = $(this);
    });
    $("#groupedMembers > div.group").each(function(){
        groupParents[$(this).attr("name")] = $(this);
    });

    $("#types > ol > li").each(function(){
        var mbr = $(this);
        this.mbrText = mbr.find("> .fullcomment .cmt").text();
        var qualName = mbr.attr("name");
        var owner = qualName.slice(0, qualName.indexOf("#"));
        var name = qualName.slice(qualName.indexOf("#") + 1);
        var inheritParent = inheritParents[owner];
        if (inheritParent != undefined) {
            var types = $("> .types > ol", inheritParent);
            if (types.length == 0) {
                inheritParent.append("<div class='types members'><h3>Type Members</h3><ol></ol></div>");
                types = $("> .types > ol", inheritParent);
            }
            var clone = mbr.clone();
            clone[0].mbrText = this.mbrText;
            types.append(clone);
        }
        var group = mbr.attr("group")
        var groupParent = groupParents[group];
        if (groupParent != undefined) {
            var types = $("> .types > ol", groupParent);
            if (types.length == 0) {
                groupParent.append("<div class='types members'><ol></ol></div>");
                types = $("> .types > ol", groupParent);
            }
            var clone = mbr.clone();
            clone[0].mbrText = this.mbrText;
            types.append(clone);
        }
    });

    $("#values > ol > li").each(function(){
        var mbr = $(this);
        this.mbrText = mbr.find("> .fullcomment .cmt").text();
        var qualName = mbr.attr("name");
        var owner = qualName.slice(0, qualName.indexOf("#"));
        var name = qualName.slice(qualName.indexOf("#") + 1);
        var inheritParent = inheritParents[owner];
        if (inheritParent != undefined) {
            var values = $("> .values > ol", inheritParent);
            if (values.length == 0) {
                inheritParent.append("<div class='values members'><h3>Value Members</h3><ol></ol></div>");
                values = $("> .values > ol", inheritParent);
            }
            var clone = mbr.clone();
            clone[0].mbrText = this.mbrText;
            values.append(clone);
        }
        var group = mbr.attr("group")
        var groupParent = groupParents[group];
        if (groupParent != undefined) {
            var values = $("> .values > ol", groupParent);
            if (values.length == 0) {
                groupParent.append("<div class='values members'><ol></ol></div>");
                values = $("> .values > ol", groupParent);
            }
            var clone = mbr.clone();
            clone[0].mbrText = this.mbrText;
            values.append(clone);
        }
    });
    $("#inheritedMembers > div.parent").each(function() {
        if ($("> div.members", this).length == 0) { $(this).remove(); };
    });
    $("#inheritedMembers > div.conversion").each(function() {
        if ($("> div.members", this).length == 0) { $(this).remove(); };
    });
    $("#groupedMembers > div.group").each(function() {
        if ($("> div.members", this).length == 0) { $(this).remove(); };
    });
};

/* filter used to take boolean scrollToMember */
function filter() {
    var query = $.trim($("#textfilter input").val()).toLowerCase();
    query = query.replace(/[-[\]{}()*+?.,\\^$|#]/g, "\\$&").replace(/\s+/g, "|");
    var queryRegExp = new RegExp(query, "i");
    var privateMembersHidden = $("#visbl > ol > li.public").hasClass("in");
    var orderingAlphabetic = $("#order > ol > li.alpha").hasClass("in");
    var orderingInheritance = $("#order > ol > li.inherit").hasClass("in");
    var orderingGroups = $("#order > ol > li.group").hasClass("in");
    var hiddenSuperclassElementsLinearization = orderingInheritance ? $("#linearization > li:gt(0)") : $("#linearization > li.out");
    var hiddenSuperclassesLinearization = hiddenSuperclassElementsLinearization.map(function() {
      return $(this).attr("name");
    }).get();
    var hiddenSuperclassElementsImplicits = orderingInheritance ? $("#implicits > li") : $("#implicits > li.out");
    var hiddenSuperclassesImplicits = hiddenSuperclassElementsImplicits.map(function() {
      return $(this).attr("name");
    }).get();

    var hideInheritedMembers;

    if (orderingAlphabetic) {
      $("#allMembers").show();
      $("#inheritedMembers").hide();
      $("#groupedMembers").hide();
      hideInheritedMembers = true;
      $("#allMembers > .members").each(filterFunc);
    } else if (orderingGroups) {
      $("#groupedMembers").show();
      $("#inheritedMembers").hide();
      $("#allMembers").hide();
      hideInheritedMembers = true;
      $("#groupedMembers  > .group > .members").each(filterFunc);
      $("#groupedMembers  > div.group").each(function() {
        $(this).show();
        if ($("> div.members", this).not(":hidden").length == 0) {
            $(this).hide();
        } else {
            $(this).show();
        }
      });
    } else if (orderingInheritance) {
      $("#inheritedMembers").show();
      $("#groupedMembers").hide();
      $("#allMembers").hide();
      hideInheritedMembers = false;
      $("#inheritedMembers > .parent > .members").each(filterFunc);
      $("#inheritedMembers > .conversion > .members").each(filterFunc);
    }


    function filterFunc() {
      var membersVisible = false;
      var members = $(this);
      members.find("> ol > li").each(function() {
        var mbr = $(this);
        if (privateMembersHidden && mbr.attr("visbl") == "prt") {
          mbr.hide();
          return;
        }
        var name = mbr.attr("name");
        // Owner filtering must not happen in "inherited from" member lists
        if (hideInheritedMembers) {
          var ownerIndex = name.indexOf("#");
          if (ownerIndex < 0) {
            ownerIndex = name.lastIndexOf(".");
          }
          var owner = name.slice(0, ownerIndex);
          for (var i = 0; i < hiddenSuperclassesLinearization.length; i++) {
            if (hiddenSuperclassesLinearization[i] == owner) {
              mbr.hide();
              return;
            }
          };
          for (var i = 0; i < hiddenSuperclassesImplicits.length; i++) {
            if (hiddenSuperclassesImplicits[i] == owner) {
              mbr.hide();
              return;
            }
          };
        }
        if (query && !(queryRegExp.test(name) || queryRegExp.test(this.mbrText))) {
          mbr.hide();
          return;
        }
        mbr.show();
        membersVisible = true;
      });

      if (membersVisible)
        members.show();
      else
        members.hide();
    };

    return false;
};

function windowTitle()
{
    try {
        parent.document.title=document.title;
    }
    catch(e) {
      // Chrome doesn't allow settings the parent's title when
      // used on the local file system.
    }
};

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

/* Dynamically injected post-processing code for the API docs */

$(document).ready(function() {
  var annotations = $("dt:contains('Annotations')").next("dd").children("span.name");
  addBadges(annotations, "AlphaComponent", ":: AlphaComponent ::", '<span class="alphaComponent badge">Alpha Component</span>');
  addBadges(annotations, "DeveloperApi", ":: DeveloperApi ::", '<span class="developer badge">Developer API</span>');
  addBadges(annotations, "Experimental", ":: Experimental ::", '<span class="experimental badge">Experimental</span>');
});

function addBadges(allAnnotations, name, tag, html) {
  var annotations = allAnnotations.filter(":contains('" + name + "')")
  var tags = $(".cmt:contains(" + tag + ")")

  // Remove identifier tags from comments
  tags.each(function(index) {
    var oldHTML = $(this).html();
    var newHTML = oldHTML.replace(tag, "");
    $(this).html(newHTML);
  });

  // Add badges to all containers
  tags.prevAll("h4.signature")
    .add(annotations.closest("div.fullcommenttop"))
    .add(annotations.closest("div.fullcomment").prevAll("h4.signature"))
    .prepend(html);
}
