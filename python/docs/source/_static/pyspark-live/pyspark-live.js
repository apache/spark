/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * "Try PySpark in your browser" - progressive enhancement for the PySpark
 * documentation. It turns code blocks marked with the "pyspark-live" CSS class
 * into runnable cells backed by Pyodide (CPython on WebAssembly) and an
 * in-browser, JVM-free Spark engine (sail-wasm).
 *
 * Lives entirely under python/docs and is loaded only when the docs are built
 * with PYSPARK_DOCS_LIVE set (see conf.py). It ships nothing into the PySpark
 * package and runs only in the reader's browser.
 *
 * How a cell runs:
 *   1. Python (sync): build the DataFrame from the cell's code and serialize its
 *      Spark Connect plan. No RPC -- plan building is entirely client-side.
 *   2. JS (async):    await sailWasm.execute_plan(planBytes) -> Arrow IPC bytes.
 *   3. Python (sync): decode the Arrow stream and render it as a table.
 * Splitting it this way keeps each Python step synchronous (Pyodide cannot block
 * on a JS Promise on the main thread) while the async wasm call stays in JS.
 *
 * Nothing heavy downloads until the reader clicks "Run".
 */

(function () {
  "use strict";

  var DEFAULTS = {
    // Pyodide distribution. 0.27.x is the series that ships pyarrow (which the
    // Spark Connect client imports); pandas, numpy and protobuf come with it too.
    pyodideIndexUrl: "https://cdn.jsdelivr.net/pyodide/v0.27.7/full/",
    // The sail-wasm loader (ES module) published as a GitHub Release asset of
    // https://github.com/HyukjinKwon/sail-wasm . Overridable via conf.py env vars.
    engineUrl:
      "https://github.com/HyukjinKwon/sail-wasm/releases/latest/download/sail_wasm.js",
    // The wasm binary. Defaults to sail_wasm_bg.wasm next to engineUrl.
    engineWasmUrl: null,
    // Packages preloaded from the Pyodide distribution before installing PySpark.
    prebuiltPackages: ["micropip", "numpy", "pandas", "pyarrow", "protobuf"],
    // Installed via micropip from PyPI. grpcio is shimmed away (not installed).
    pipPackages: ["pyspark"],
    // Optional archives unpacked into the Pyodide filesystem before bootstrap,
    // each {url, format, extractDir}. Lets a deployment provide PySpark (and
    // friends) as hosted archives instead of, or in addition to, PyPI. extractDir
    // is added to sys.path. Empty by default.
    setupArchives: []
  };

  var CONFIG = Object.assign({}, DEFAULTS, window.PYSPARK_LIVE_CONFIG || {});
  if (!CONFIG.engineWasmUrl) {
    CONFIG.engineWasmUrl = CONFIG.engineUrl.replace(
      /sail_wasm\.js(\?.*)?$/,
      "sail_wasm_bg.wasm"
    );
  }

  function assetBaseUrl() {
    var current =
      document.currentScript ||
      document.querySelector('script[src*="pyspark-live/pyspark-live.js"]');
    if (current && current.src) {
      return current.src.replace(/pyspark-live\.js(\?.*)?$/, "");
    }
    return "";
  }

  var ASSET_BASE = assetBaseUrl();

  // Lazily-created runtime: a ready Pyodide with PySpark installed and the
  // bootstrap helpers defined, plus the sail-wasm execute_plan function.
  var runtimeReady = null;

  function loadScript(src) {
    return new Promise(function (resolve, reject) {
      var s = document.createElement("script");
      s.src = src;
      s.onload = resolve;
      s.onerror = function () {
        reject(new Error("Failed to load " + src));
      };
      document.head.appendChild(s);
    });
  }

  // Import the sail-wasm ES module without depending on the server's MIME type
  // (GitHub Releases serve assets as octet-stream, which blocks a direct
  // import()). We fetch the source, import it from a blob URL, and point the
  // initializer at the wasm binary explicitly.
  async function loadEngine() {
    var source = await fetch(CONFIG.engineUrl).then(function (r) {
      if (!r.ok) throw new Error("Could not fetch sail-wasm (" + r.status + ")");
      return r.text();
    });
    var blobUrl = URL.createObjectURL(
      new Blob([source], { type: "text/javascript" })
    );
    var engine = await import(/* webpackIgnore: true */ blobUrl);
    await engine.default({ module_or_path: CONFIG.engineWasmUrl });
    if (typeof engine.start === "function") {
      try {
        engine.start();
      } catch (e) {
        /* start() may already have run during init */
      }
    }
    return engine.execute_plan;
  }

  function bootstrapRuntime(onStatus) {
    if (runtimeReady) {
      return runtimeReady;
    }
    runtimeReady = (async function () {
      onStatus("Downloading the Python runtime (one-time, ~10s)...");
      await loadScript(CONFIG.pyodideIndexUrl + "pyodide.js");
      var pyodide = await loadPyodide({ indexURL: CONFIG.pyodideIndexUrl });

      onStatus("Installing PySpark and its dependencies...");
      await pyodide.loadPackage(CONFIG.prebuiltPackages);

      if (CONFIG.setupArchives && CONFIG.setupArchives.length) {
        for (var a = 0; a < CONFIG.setupArchives.length; a++) {
          var archive = CONFIG.setupArchives[a];
          var buffer = await fetch(archive.url).then(function (r) {
            if (!r.ok) throw new Error("Could not fetch " + archive.url);
            return r.arrayBuffer();
          });
          var options = archive.extractDir ? { extractDir: archive.extractDir } : undefined;
          pyodide.unpackArchive(buffer, archive.format || "zip", options);
          if (archive.extractDir) {
            pyodide.runPython(
              "import sys; sys.path.insert(0, " + JSON.stringify(archive.extractDir) + ")"
            );
          }
        }
      }

      if (CONFIG.pipPackages && CONFIG.pipPackages.length) {
        var micropip = pyodide.pyimport("micropip");
        await micropip.install(CONFIG.pipPackages);
      }

      onStatus("Starting the in-browser Spark engine...");
      var executePlan = await loadEngine();

      onStatus("Wiring up PySpark...");
      var bootstrap = await fetch(ASSET_BASE + "bootstrap.py").then(function (r) {
        if (!r.ok) throw new Error("Could not fetch bootstrap.py (" + r.status + ")");
        return r.text();
      });
      await pyodide.runPythonAsync(bootstrap);

      return { pyodide: pyodide, executePlan: executePlan };
    })().catch(function (err) {
      runtimeReady = null; // allow retry after a transient failure
      throw err;
    });
    return runtimeReady;
  }

  function bytesToBase64(bytes) {
    var binary = "";
    var chunk = 0x8000;
    for (var i = 0; i < bytes.length; i += chunk) {
      binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));
    }
    return btoa(binary);
  }

  function base64ToBytes(b64) {
    var binary = atob(b64);
    var bytes = new Uint8Array(binary.length);
    for (var i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
  }

  function cellSource(container) {
    var pre = container.querySelector("pre");
    if (!pre) return "";
    var clone = pre.cloneNode(true);
    clone.querySelectorAll(".linenos, .gp, button").forEach(function (el) {
      el.remove();
    });
    return clone.textContent.replace(/\n+$/, "");
  }

  function showOutput(elements, text, isError) {
    elements.output.textContent = text;
    elements.output.classList.add("pyspark-live-visible");
    elements.output.classList.toggle("pyspark-live-error", !!isError);
  }

  async function runCell(elements) {
    elements.button.disabled = true;
    elements.output.classList.remove("pyspark-live-visible");
    var setStatus = function (msg) {
      elements.status.textContent = msg;
    };

    try {
      var rt = await bootstrapRuntime(setStatus);
      var pyodide = rt.pyodide;

      setStatus("Building the query plan...");
      pyodide.globals.set("_pyspark_live_src", cellSource(elements.container));
      var planResult = JSON.parse(
        pyodide.runPython("pyspark_live_plan_b64(_pyspark_live_src)")
      );
      if (!planResult.ok) {
        setStatus("");
        showOutput(elements, planResult.error, true);
        return;
      }

      var text = planResult.stdout || "";
      if (planResult.plan) {
        setStatus("Executing in sail-wasm...");
        var arrow = await rt.executePlan(base64ToBytes(planResult.plan));
        pyodide.globals.set("_pyspark_live_arrow", bytesToBase64(arrow));
        var rendered = JSON.parse(
          pyodide.runPython("pyspark_live_render_b64(_pyspark_live_arrow)")
        );
        if (!rendered.ok) {
          setStatus("");
          showOutput(elements, (text ? text + "\n" : "") + rendered.error, true);
          return;
        }
        text += (text && rendered.text ? "\n" : "") + rendered.text;
      }

      setStatus("");
      showOutput(elements, text.length ? text : "(no output)", false);
    } catch (err) {
      setStatus("");
      showOutput(elements, String(err && err.message ? err.message : err), true);
    } finally {
      elements.button.disabled = false;
    }
  }

  function enhance(container) {
    if (container.dataset.pysparkLiveReady) return;
    container.dataset.pysparkLiveReady = "1";

    var toolbar = document.createElement("div");
    toolbar.className = "pyspark-live-toolbar";

    var button = document.createElement("button");
    button.className = "pyspark-live-run";
    button.type = "button";
    button.textContent = "Run";

    var status = document.createElement("span");
    status.className = "pyspark-live-status";

    var output = document.createElement("div");
    output.className = "pyspark-live-output";

    toolbar.appendChild(button);
    toolbar.appendChild(status);
    container.parentNode.insertBefore(toolbar, container.nextSibling);
    toolbar.parentNode.insertBefore(output, toolbar.nextSibling);

    var elements = {
      container: container,
      button: button,
      status: status,
      output: output
    };
    button.addEventListener("click", function () {
      runCell(elements);
    });
  }

  function init() {
    document.querySelectorAll(".pyspark-live").forEach(enhance);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
