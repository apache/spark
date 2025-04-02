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

package org.apache.spark

/**
 * DataFrame-based machine learning APIs to let users quickly assemble and configure practical
 * machine learning pipelines.
 *
 * @groupname param Parameters
 * @groupdesc param A list of (hyper-)parameter keys this algorithm can take. Users can set and get
 *            the parameter values through setters and getters, respectively.
 * @groupprio param -5
 *
 * @groupname setParam Parameter setters
 * @groupprio setParam 5
 *
 * @groupname getParam Parameter getters
 * @groupprio getParam 6
 *
 * @groupname expertParam (expert-only) Parameters
 * @groupdesc expertParam A list of advanced, expert-only (hyper-)parameter keys this algorithm can
 *            take. Users can set and get the parameter values through setters and getters,
 *            respectively.
 * @groupprio expertParam 7
 *
 * @groupname expertSetParam (expert-only) Parameter setters
 * @groupprio expertSetParam 8
 *
 * @groupname expertGetParam (expert-only) Parameter getters
 * @groupprio expertGetParam 9
 *
 * @groupname Ungrouped Members
 * @groupprio Ungrouped 0
 */
package object ml
