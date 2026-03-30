#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Image for generating Spark Connect protobuf files. Based on Ubuntu 24.04.
FROM ubuntu:noble
LABEL org.opencontainers.image.authors="Apache Spark project <dev@spark.apache.org>"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.ref.name="Apache Spark Infra Image For Spark Connect CodeGen"
# Overwrite this label to avoid exposing the underlying Ubuntu OS version label
LABEL org.opencontainers.image.version=""

ENV FULL_REFRESH_DATE=20260311

ENV DEBIAN_FRONTEND=noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN=true
ENV OPENSSL_FORCE_FIPS_MODE=0

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    python3.12 \
    python3.12-venv \
    && apt-get autoremove --purge -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install buf binary from GitHub releases
# See https://buf.build/docs/cli/installation/#github
ARG BUF_VERSION="1.66.1"
RUN curl -fsSL "https://github.com/bufbuild/buf/releases/download/v$BUF_VERSION/buf-Linux-$(uname -m)" -o /usr/local/bin/buf && \
    chmod +x /usr/local/bin/buf

# Setup virtual environment and install Python dependencies
ENV VIRTUAL_ENV=/opt/spark-venv
RUN python3.12 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Keep these versions in sync with the pinned versions in dev/requirements.txt
RUN python3.12 -m pip install \
    'mypy==1.19.1' \
    'mypy-protobuf==3.3.0' \
    'ruff==0.14.8'

# Mount the Spark repo at /spark
WORKDIR /spark

ENTRYPOINT [ "/spark/dev/connect-gen-protos.sh" ]
