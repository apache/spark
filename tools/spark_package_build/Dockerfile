# Spark binary package builder (.tgz) -- compilation environment, runs on a Buildkite agent or any x86_64 Linux Docker host.
# Java is chosen per Spark version: 8 for 3.5.x, 21 for 4.x (see README).
ARG JAVA_VERSION=8
ARG BASE_IMAGE=eclipse-temurin:${JAVA_VERSION}-jdk-jammy
FROM ${BASE_IMAGE}

ARG MAVEN_VERSION=3.9.9

RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Maven, checked against the checksum published next to the archive.
RUN curl -fsSL -o /tmp/maven.tgz "https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz" \
    && curl -fsSL -o /tmp/maven.tgz.sha512 "https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz.sha512" \
    && echo "$(cut -d' ' -f1 /tmp/maven.tgz.sha512)  /tmp/maven.tgz" | sha512sum -c - \
    && mkdir -p /opt/maven && tar -xzf /tmp/maven.tgz -C /opt/maven --strip-components=1 \
    && rm -f /tmp/maven.tgz /tmp/maven.tgz.sha512
ENV PATH="/opt/maven/bin:${PATH}"
ENV MAVEN_OPTS="-Xss64m -Xmx4g -XX:ReservedCodeCacheSize=1g"

# Maven settings for an optional mirror (MAVEN_MIRROR_URL, set at run time by the pipeline). Maven Central rate-limits
# and Scala downloads were refused in the manual builds, so the pipeline should set it.
COPY settings.xml /settings.xml

WORKDIR /build

COPY build-spark-package.sh /build/build-spark-package.sh
RUN chmod +x /build/build-spark-package.sh

# VERSIONS.md is validated at runtime: numeric affirm-versions must be documented with a matching commit SHA.
COPY VERSIONS.md /build/VERSIONS.md

ENTRYPOINT ["/build/build-spark-package.sh"]
