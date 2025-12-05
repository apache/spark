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

// Compile: javac --source 8 --target 8 HelloWithPackage.java

package my.custom.pkg;

public class HelloWithPackage {
    String name = "there";

    public HelloWithPackage() {
    }

    public HelloWithPackage(String name) {
        this.name = name;
    }

    public String msg() {
        return "Hello " + name + "! Nice to meet you!";
    }
}
