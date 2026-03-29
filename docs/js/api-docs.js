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

/* global $, MathJax */

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
  tags.each(function(_ignored_index) {
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

$(document).ready(function() {
  var script = document.createElement('script');
  script.type = 'text/javascript';
  script.async = true;
  script.onload = function(){
    MathJax.Hub.Config({
      displayAlign: "left",
      tex2jax: {
        inlineMath: [ ["$", "$"], ["\\(","\\)"] ],
        displayMath: [ ["$$","$$"], ["\\[", "\\]"] ],
        processEscapes: true,
        skipTags: ['script', 'noscript', 'style', 'textarea', 'pre', 'a']
      }
    });
  };
  script.src = ('https:' == document.location.protocol ? 'https://' : 'http://') +
                'cdnjs.cloudflare.com/ajax/libs/mathjax/2.7.1/MathJax.js' +
                '?config=TeX-AMS-MML_HTMLorMML';
  document.getElementsByTagName('head')[0].appendChild(script);
});
