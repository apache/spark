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
LABEL org.opencontainers.image.ref.name="Apache Spark Infra Image For PySpark with Python 3.14"
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

# Install Python 3.14
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update && apt-get install -y \
    python3.14 \
    python3.14-venv \
    && apt-get autoremove --purge -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Setup virtual environment
ENV VIRTUAL_ENV=/opt/spark-venv
RUN python3.14 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

ARG BASIC_PIP_PKGS="numpy pyarrow>=22.0.0 six==1.16.0 pandas==2.3.3 scipy plotly<6.0.0 mlflow>=2.8.1 coverage matplotlib openpyxl memory-profiler>=0.61.0 scikit-learn>=1.3.2 pystack>=1.6.0 psutil"
ARG CONNECT_PIP_PKGS="grpcio==1.76.0 grpcio-status==1.76.0 protobuf==6.33.5 googleapis-common-protos==1.71.0 zstandard==0.25.0 graphviz==0.20.3"

RUN python3.14 -m pip install $BASIC_PIP_PKGS unittest-xml-reporting $CONNECT_PIP_PKGS lxml && \
    python3.14 -m pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu && \
    python3.14 -m pip install torcheval && \
    python3.14 -m pip cache purge
