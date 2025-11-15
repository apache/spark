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
LABEL org.opencontainers.image.ref.name="Apache Spark Infra Image For PySpark with PyPy 3.10"
# Overwrite this label to avoid exposing the underlying Ubuntu OS version label
LABEL org.opencontainers.image.version=""

ENV FULL_REFRESH_DATE=20241212

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
    libwebp-dev \
    libxml2-dev \
    openjdk-17-jdk-headless \
    pkg-config \
    qpdf \
    tzdata \
    software-properties-common \
    wget \
    zlib1g-dev \
    rustc \
    cargo \
    && apt-get autoremove --purge -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


#need a newer cmake version for building pyarrow
RUN wget https://github.com/Kitware/CMake/releases/download/v3.28.3/cmake-3.28.3-linux-x86_64.sh \
    -q -O /tmp/cmake-install.sh \
    && chmod u+x /tmp/cmake-install.sh \
    && /tmp/cmake-install.sh --skip-license --prefix=/usr/local \
    && rm /tmp/cmake-install.sh
    
ARG BASIC_PIP_PKGS="numpy pyarrow>=22.0.0 six==1.16.0 pandas==2.3.3 scipy plotly<6.0.0 mlflow>=2.8.1 coverage matplotlib openpyxl memory-profiler>=0.61.0 scikit-learn>=1.3.2"
# Python deps for Spark Connect
ARG CONNECT_PIP_PKGS="grpcio==1.76.0 grpcio-status==1.76.0 protobuf==6.33.0 googleapis-common-protos==1.71.0 zstandard==0.25.0 graphviz==0.20.3"

RUN add-apt-repository ppa:pypy/ppa
RUN mkdir -p /usr/local/pypy/pypy3.10 && \
    curl -sqL https://downloads.python.org/pypy/pypy3.10-v7.3.17-linux64.tar.bz2 | tar xjf - -C /usr/local/pypy/pypy3.10 --strip-components=1 && \
    ln -sf /usr/local/pypy/pypy3.10/bin/pypy /usr/local/bin/pypy3.10 && \
    ln -sf /usr/local/pypy/pypy3.10/bin/pypy /usr/local/bin/pypy3
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | pypy3
RUN pypy3 -m pip install --ignore-installed 'blinker>=1.6.2' # mlflow needs this
RUN pypy3 -m pip install --ignore-installed 'six==1.16.0'  # Avoid `python3-six` installation
RUN pypy3 -m pip install $BASIC_PIP_PKGS unittest-xml-reporting $CONNECT_PIP_PKGS && \
    pypy3 -m pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu && \
    pypy3 -m pip install deepspeed torcheval && \
    pypy3 -m pip cache purge