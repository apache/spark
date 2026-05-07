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


export { addScrollButton };

function createBtn(top = true) {
  const button = document.createElement('div');
  button.classList.add('scroll-btn-half');
  const className = top ? 'scroll-btn-top' : 'scroll-btn-bottom';
  button.classList.add(className);
  button.addEventListener('click', function () {
    window.scrollTo({
      top: top ? 0 : document.body.scrollHeight,
      behavior: 'smooth' });
  })
  return button;
}

function addScrollButton() {
  const containerClass = 'scroll-btn-container';
  if (document.querySelector(`.${containerClass}`)) {
    return;
  }
  const container = document.createElement('div');
  container.className = containerClass;
  container.appendChild(createBtn());
  container.appendChild(createBtn(false));
  document.body.appendChild(container);
}

document.addEventListener('DOMContentLoaded', function () {
  addScrollButton();
});
