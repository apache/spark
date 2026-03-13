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

import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const staticDir = join(__dirname, '../../core/src/main/resources/org/apache/spark/ui/static');

function readStatic(filename) {
  return readFileSync(join(staticDir, filename), 'utf-8');
}

describe("loading overlay", () => {
  test("overlay has expected DOM structure", () => {
    document.body.innerHTML = `
      <div id="loading-overlay" class="position-fixed top-0 start-0 w-100 h-100 d-flex justify-content-center align-items-center d-none">
        <div class="text-center">
          <div class="spinner-border text-primary" role="status">
            <span class="visually-hidden">Loading...</span>
          </div>
          <h3 class="mt-2">Loading...</h3>
        </div>
      </div>`;

    const overlay = document.getElementById("loading-overlay");
    expect(overlay).not.toBeNull();
    expect(overlay.classList.contains("d-none")).toBe(true);
    expect(overlay.classList.contains("position-fixed")).toBe(true);

    const spinner = overlay.querySelector(".spinner-border");
    expect(spinner).not.toBeNull();
    expect(spinner.getAttribute("role")).toBe("status");
    expect(spinner.querySelector(".visually-hidden").textContent).toBe("Loading...");
  });

  test("overlay can be shown and hidden via d-none class", () => {
    document.body.innerHTML = `
      <div id="loading-overlay" class="position-fixed top-0 start-0 w-100 h-100 d-flex justify-content-center align-items-center d-none">
        <div class="text-center">
          <div class="spinner-border text-primary" role="status">
            <span class="visually-hidden">Loading...</span>
          </div>
          <h3 class="mt-2">Loading...</h3>
        </div>
      </div>`;

    const overlay = document.getElementById("loading-overlay");

    // Initially hidden
    expect(overlay.classList.contains("d-none")).toBe(true);

    // Show overlay
    overlay.classList.remove("d-none");
    expect(overlay.classList.contains("d-none")).toBe(false);

    // Hide overlay
    overlay.classList.add("d-none");
    expect(overlay.classList.contains("d-none")).toBe(true);
  });

  test("CSS has dark mode support for overlay", () => {
    const css = readStatic("webui.css");
    expect(css).toContain("#loading-overlay");
    expect(css).toContain('[data-bs-theme="dark"] #loading-overlay');
  });
});
