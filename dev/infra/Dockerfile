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
# Focal (20.04) reached end-of-standard-support and its archives/PPAs have
# become unreliable, so this base image was moved to Jammy. See SPARK-56808.
# See also in https://hub.docker.com/_/ubuntu
FROM ubuntu:jammy-20240911.1

ENV FULL_REFRESH_DATE 20260510

ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN true

ARG APT_INSTALL="apt-get install --no-install-recommends -y"

RUN echo 'deb https://mirrors.edge.kernel.org/ubuntu jammy main restricted universe multiverse' > /etc/apt/sources.list.d/mirror.list && \
    echo 'deb https://mirrors.edge.kernel.org/ubuntu jammy-updates main restricted universe multiverse' >> /etc/apt/sources.list.d/mirror.list && \
    echo 'deb https://mirrors.edge.kernel.org/ubuntu jammy-security main restricted universe multiverse' >> /etc/apt/sources.list.d/mirror.list

RUN apt-get clean
RUN apt-get update
RUN $APT_INSTALL software-properties-common git libxml2-dev pkg-config curl wget openjdk-8-jdk libpython3-dev python3-pip python3-setuptools gnupg ca-certificates dirmngr
# branch-3.5 tests use python3.8 and python3.9 explicitly; Jammy ships python3.10 by default,
# so pull 3.8/3.9 from the deadsnakes PPA (same approach used by master for python3.9).
# gnupg/ca-certificates/dirmngr must be installed first so add-apt-repository can import the PPA key.
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update
RUN $APT_INSTALL python3.8 python3.8-distutils python3.9 python3.9-distutils
RUN update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java

# The generic get-pip.py requires Python 3.10+, so use the version-specific bootstrap URL.
RUN curl -sS https://bootstrap.pypa.io/pip/3.9/get-pip.py | python3.9
RUN curl -sS https://bootstrap.pypa.io/pip/3.8/get-pip.py | python3.8

RUN $APT_INSTALL gfortran libopenblas-dev liblapack-dev
RUN $APT_INSTALL build-essential

# Upgrade pypy from 3.8 to 3.10 to gain Python 3.10 AST nodes (MatchStar, MatchAs).
# Without them, the scipy build chain (pythran → beniget) fails with
# `AttributeError: module 'gast' has no attribute 'MatchStar'` even with pinned gast.
RUN mkdir -p /usr/local/pypy/pypy3.10 && \
    curl -sqL https://downloads.python.org/pypy/pypy3.10-v7.3.17-linux64.tar.bz2 | tar xjf - -C /usr/local/pypy/pypy3.10 --strip-components=1 && \
    ln -sf /usr/local/pypy/pypy3.10/bin/pypy /usr/local/bin/pypy3.10 && \
    ln -sf /usr/local/pypy/pypy3.10/bin/pypy /usr/local/bin/pypy3

RUN curl -sS https://bootstrap.pypa.io/get-pip.py | pypy3

RUN $APT_INSTALL pandoc
RUN echo 'deb https://cloud.r-project.org/bin/linux/ubuntu jammy-cran40/' >> /etc/apt/sources.list
RUN gpg --keyserver hkps://keyserver.ubuntu.com --recv-key E298A3A825C0D65DFD57CBB651716619E084DAB9
RUN gpg -a --export E084DAB9 | apt-key add -
RUN add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu jammy-cran40/'
RUN apt update
RUN $APT_INSTALL r-base libcurl4-openssl-dev qpdf libssl-dev zlib1g-dev
RUN Rscript -e "install.packages(c('remotes', 'knitr', 'markdown', 'rmarkdown', 'testthat', 'e1071', 'survival', 'arrow', 'roxygen2', 'xml2'), repos='https://cloud.r-project.org/')"

# See more in SPARK-39959, roxygen2 < 7.2.1
# libuv1-dev is required by the R `fs` package (transitive dep of testthat/rmarkdown/etc.).
RUN apt-get update && apt-get install -y libcurl4-openssl-dev libgit2-dev libssl-dev libxml2-dev \
          libfontconfig1-dev libharfbuzz-dev libfribidi-dev libfreetype6-dev libpng-dev \
          libtiff5-dev libjpeg-dev libuv1-dev
RUN Rscript -e "install.packages(c('remotes'), repos='https://cloud.r-project.org/')"
RUN Rscript -e "remotes::install_version('roxygen2', version='7.2.0', repos='https://cloud.r-project.org')"

# See more in SPARK-39735
ENV R_LIBS_SITE "/usr/local/lib/R/site-library:${R_LIBS_SITE}:/usr/lib/R/library"

# pypy3.10's ast module defines MatchStar/MatchAs, so gast/beniget/pythran/scipy
# build out of the box without the gast>=0.5.3 PIP_CONSTRAINT workaround needed for pypy3.8.
RUN pypy3 -m pip install numpy 'pandas<=2.0.3' scipy coverage matplotlib
RUN python3.9 -m pip install 'numpy==1.25.1' 'pyarrow==12.0.1' 'pandas<=2.0.3' scipy unittest-xml-reporting plotly>=4.8 'mlflow>=2.3.1' coverage 'matplotlib==3.7.2' openpyxl 'memory-profiler==0.60.0' 'scikit-learn==1.1.*'

# Add Python deps for Spark Connect.
RUN python3.9 -m pip install 'grpcio>=1.48,<1.57' 'grpcio-status>=1.48,<1.57' 'protobuf==3.20.3' 'googleapis-common-protos==1.56.4'

# Add torch as a testing dependency for TorchDistributor
RUN python3.9 -m pip install 'torch==2.0.1' 'torchvision==0.15.2' torcheval
