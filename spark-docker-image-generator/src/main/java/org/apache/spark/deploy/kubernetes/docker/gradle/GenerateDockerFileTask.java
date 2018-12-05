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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.gradle.api.DefaultTask;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

public class GenerateDockerFileTask extends DefaultTask {

    private File srcDockerFile;
    private File destDockerFile;
    private Property<String> baseImage;

    public final void setSrcDockerFile(File srcDockerFile) {
        this.srcDockerFile = srcDockerFile;
    }

    public final void setDestDockerFile(File destDockerFile) {
        this.destDockerFile = destDockerFile;
    }

    public final void setBaseImage(Property<String> baseImage) {
        this.baseImage = baseImage;
    }

    @InputFile
    public final File getSrcDockerFile() {
        return srcDockerFile;
    }

    @OutputFile
    public final File getDestDockerFile() {
        return destDockerFile;
    }

    @Input
    public final Property<String> getBaseImage() {
        return baseImage;
    }

    @TaskAction
    public final void generateDockerFile() throws IOException {
        if (!baseImage.isPresent()) {
            Files.copy(srcDockerFile.toPath(), destDockerFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } else {
            File currentSrcDockerFile = getSrcDockerFile();
            File currentDestDockerFile = getDestDockerFile();
            List<String> fileLines;
            try (Stream<String> rawLines = Files.lines(currentSrcDockerFile.toPath(), StandardCharsets.UTF_8)) {
                fileLines = rawLines.map(line -> {
                    if (line.equals("FROM openjdk:8-alpine")) {
                        return String.format("FROM %s", baseImage.get());
                    } else {
                        return line;
                    }
                }).collect(Collectors.toList());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Files.write(currentDestDockerFile.toPath(), fileLines, StandardCharsets.UTF_8);
        }
    }
}
