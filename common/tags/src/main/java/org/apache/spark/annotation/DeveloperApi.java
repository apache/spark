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

package org.apache.spark.annotation;

import java.lang.annotation.*;

/**
 * A lower-level, unstable API intended for developers.
 *
 * Developer API's might change or be removed in minor versions of Spark.
 *
 * NOTE: If there exists a Scaladoc comment that immediately precedes this annotation, the first
 * line of the comment must be ":: DeveloperApi ::" with no trailing blank line. This is because
 * of the known issue that Scaladoc displays only either the annotation or the comment, whichever
 * comes first.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER,
        ElementType.CONSTRUCTOR, ElementType.LOCAL_VARIABLE, ElementType.PACKAGE})
public @interface DeveloperApi {}
