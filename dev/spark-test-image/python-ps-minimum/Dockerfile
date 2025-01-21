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

# Image for building and testing Spark branches. Based on Ubuntu 22.04.
# See also in https://hub.docker.com/_/ubuntu
FROM ubuntu:jammy-20240911.1
LABEL org.opencontainers.image.authors="Apache Spark project <dev@spark.apache.org>"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.ref.name="Apache Spark Infra Image For Pandas API on Spark with old dependencies"
# Overwrite this label to avoid exposing the underlying Ubuntu OS version label
LABEL org.opencontainers.image.version=""

ENV FULL_REFRESH_DATE=20250102

ENV DEBIAN_FRONTEND=noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN=true

RUN apt-get update && apt-get install -y \
    build-essential \
    ca-certificates \
    curl \
    gfortran \
    git \
    gnupg \
    libcurl4-openssl-dev \
    libfontconfig1-dev \
    libfreetype6-dev \
    libfribidi-dev \
    libgit2-dev \
    libharfbuzz-dev \
    libjpeg-dev \
    liblapack-dev \
    libopenblas-dev \
    libpng-dev \
    libpython3-dev \
    libssl-dev \
    libtiff5-dev \
    libxml2-dev \
    openjdk-17-jdk-headless \
    pkg-config \
    qpdf \
    tzdata \
    software-properties-common \
    wget \
    zlib1g-dev


# Should keep the installation consistent with https://apache.github.io/spark/api/python/getting_started/install.html

# Install Python 3.9
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update && apt-get install -y \
    python3.9 \
    python3.9-distutils \
    && apt-get autoremove --purge -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


ARG BASIC_PIP_PKGS="pyarrow==11.0.0 pandas==2.2.0 six==1.16.0 numpy scipy coverage unittest-xml-reporting"
# Python deps for Spark Connect
ARG CONNECT_PIP_PKGS="grpcio==1.67.0 grpcio-status==1.67.0 googleapis-common-protos==1.65.0 graphviz==0.20 protobuf"

# Install Python 3.9 packages
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.9
RUN python3.9 -m pip install --force $BASIC_PIP_PKGS $CONNECT_PIP_PKGS && \
    python3.9 -m pip cache purge
