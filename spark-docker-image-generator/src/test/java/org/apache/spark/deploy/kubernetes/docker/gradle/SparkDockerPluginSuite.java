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

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ExecCreation;
import com.spotify.docker.client.messages.ImageInfo;
import java.io.File;
import java.nio.file.Paths;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.gradle.testkit.runner.GradleRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class SparkDockerPluginSuite {

    private static final File TEST_PROJECT_DIR = Paths.get("src/test/resources/plugin-test-project").toFile();

    private String dockerTag;

    @Before
    public void before() {
        dockerTag = UUID.randomUUID().toString().replaceAll("-", "");
    }

    @After
    public void after() throws Exception {
        try (DockerClient dockerClient = DefaultDockerClient.fromEnv().build()) {
            ImageInfo taggedImageInfo = dockerClient.inspectImage(
                    String.format("docker.palantir.test/spark/spark-test-app:%s", dockerTag));
            dockerClient.removeImage(taggedImageInfo.id(), true, false);
        }
    }

    @Test
    public void testSetupProject() throws Exception {
        GradleRunner runner = GradleRunner.create()
                .withPluginClasspath()
                .withArguments(
                        "clean",
                        "sparkDockerTag",
                        String.format("-Ddocker-tag=%s", dockerTag),
                        "--stacktrace",
                        "--info")
                .withProjectDir(TEST_PROJECT_DIR)
                .forwardOutput();
        runner.build();

        try (DockerClient dockerClient = DefaultDockerClient.fromEnv().build()) {
            ImageInfo taggedImageInfo = dockerClient.inspectImage(
                    String.format("docker.palantir.test/spark/spark-test-app:%s", dockerTag));
            Assertions.assertThat(taggedImageInfo).isNotNull();
            ContainerConfig containerConfig = ContainerConfig.builder()
                    .entrypoint("bash")
                    .cmd("-c", "while :; do sleep 1000; done")
                    .image(taggedImageInfo.id())
                    .build();
            String containerId = dockerClient.createContainer(containerConfig).id();
            try {
                dockerClient.startContainer(containerId);
                expectFilesInDir(
                        dockerClient,
                        containerId,
                        "/opt/spark/jars",
                        "guava-21.0.jar",
                        "commons-io-2.4.jar",
                        "plugin-test-project-1.0.jar");
                expectFilesInDir(
                        dockerClient,
                        containerId,
                        "/opt/",
                        "spark",
                        "entrypoint.sh");
                expectFilesInDir(
                        dockerClient,
                        containerId,
                        "/opt/spark/bin",
                        "spark-submit");
            } finally {
                destroyContainer(dockerClient, containerId);
            }
        }
    }

    private void expectFilesInDir(
            DockerClient dockerClient,
            String containerId,
            String path,
            String... expectedFiles) throws DockerException, InterruptedException {
        String[] listFilesCommand = new String[]{"bash", "-c", String.format("ls %s", path)};
        ExecCreation listFilesExec = dockerClient.execCreate(
                containerId,
                listFilesCommand,
                DockerClient.ExecCreateParam.attachStdout(),
                DockerClient.ExecCreateParam.attachStderr());
        try (LogStream listFilesLogs = dockerClient.execStart(listFilesExec.id())) {
            String output = listFilesLogs.readFully();
            Assertions.assertThat(output.split("\\s+")).contains(expectedFiles);
        }
    }

    private void destroyContainer(DockerClient dockerClient, String containerId)
            throws DockerException, InterruptedException {
        try {
            dockerClient.killContainer(containerId);
        } finally {
            dockerClient.removeContainer(containerId);
        }
    }
}
