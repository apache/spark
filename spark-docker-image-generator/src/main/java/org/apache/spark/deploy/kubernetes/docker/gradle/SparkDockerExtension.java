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
package org.apache.spark.deploy.kubernetes.docker.gradle;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.gradle.api.Project;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.SetProperty;

public class SparkDockerExtension {

    private final Property<String> baseImage;
    private final Property<String> imageName;
    private final SetProperty<String> tags;

    public SparkDockerExtension(Project project) {
        this.baseImage = project.getObjects().property(String.class);
        this.imageName = project.getObjects().property(String.class);
        this.tags = project.getObjects().setProperty(String.class);
    }

    public final Property<String> getBaseImage() {
        return baseImage;
    }

    public final Property<String> getImageName() {
        return imageName;
    }

    public final SetProperty<String> getTags() {
        return tags;
    }

    @SuppressWarnings("HiddenField")
    public final void baseImage(String baseImage) {
        this.baseImage.set(baseImage);
    }

    @SuppressWarnings("HiddenField")
    public final void imageName(String imageName) {
        this.imageName.set(imageName);
    }

    @SuppressWarnings("HiddenField")
    public final void tags(Collection<String> tags) {
        this.tags.set(tags);
    }

    @SuppressWarnings("HiddenField")
    public final void tags(String... tags) {
        this.tags.set(Stream.of(tags).collect(Collectors.toSet()));
    }
}
