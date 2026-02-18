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

# Image for building and testing Spark branches. Based on Ubuntu 24.04.
# See also in https://hub.docker.com/_/ubuntu
FROM ubuntu:noble
LABEL org.opencontainers.image.authors="Apache Spark project <dev@spark.apache.org>"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.ref.name="Apache Spark Infra Image For PySpark with Python 3.13 (no GIL)"
# Overwrite this label to avoid exposing the underlying Ubuntu OS version label
LABEL org.opencontainers.image.version=""

ENV FULL_REFRESH_DATE=20260210

ENV DEBIAN_FRONTEND=noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN=true

RUN apt-get update && apt-get install -y \
    build-essential \
    ca-certificates \
    curl \
    gfortran \
    git \
    gnupg \
    libgit2-dev \
    liblapack-dev \
    libopenblas-dev \
    libssl-dev \
    openjdk-17-jdk-headless \
    pkg-config \
    tzdata \
    software-properties-common \
    zlib1g-dev

# Install Python 3.14 (no GIL)
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update && apt-get install -y \
    python3.14-nogil \
    python3.14-venv \
    && apt-get autoremove --purge -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Setup virtual environment
ENV VIRTUAL_ENV=/opt/spark-venv
RUN python3.14t -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# TODO: Add BASIC_PIP_PKGS and CONNECT_PIP_PKGS when it supports Python 3.14 free threaded
# TODO: Add lxml, grpcio, grpcio-status back when they support Python 3.14 free threaded
RUN python3.14t -m pip install 'numpy>=2.1' 'pyarrow>=19.0.0' 'six==1.16.0' 'pandas==2.3.3' 'pystack>=1.6.0' scipy coverage matplotlib openpyxl jinja2 psutil && \
    python3.14t -m pip cache purge
