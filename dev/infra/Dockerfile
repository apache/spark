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
FROM ubuntu:jammy

ENV FULL_REFRESH_DATE 20260420

ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN true

ARG APT_INSTALL="apt-get install -y"

ENV PATH "$PATH:/usr/local/bin"

RUN timeout 5 bash -c 'exec 3<>/dev/tcp/archive.ubuntu.com/80 && printf "HEAD /ubuntu/ HTTP/1.1\r\nHost: archive.ubuntu.com\r\nConnection: close\r\n\r\n" >&3 && IFS= read -r s <&3 && [[ "$s" =~ ^HTTP/.*[[:space:]](2|3)[0-9][0-9] ]]' || find /etc/apt -type f \( -name '*.list' -o -name '*.sources' \) -exec sed -i.bak -e 's|archive\.ubuntu\.com|mirror.fcix.net|g' -e 's|security\.ubuntu\.com|mirror.fcix.net|g' {} +
RUN apt-get clean && apt-get update
RUN PKGS="software-properties-common git libxml2-dev pkg-config curl wget openjdk-8-jdk libpython3-dev python3-pip python3-setuptools build-essential gfortran libopenblas-dev liblapack-dev gpg gpg-agent software-properties-common gcc g++ make libc6-dev libffi-dev libcurl4-openssl-dev libssl-dev openssl"; $APT_INSTALL $PKGS || (apt-get update && $APT_INSTALL $PKGS)
RUN update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java

# We also want Python 3.8 since that's the oldest supported version for Spark 3.5
# Also ubuntu is under a DDoS so retry adding, and finally fallback to python.org 3.8 release
RUN ((add-apt-repository -y ppa:deadsnakes/ppa ||  add-apt-repository -y ppa:deadsnakes/ppa) && \
    (apt-get update || apt-get update) && \
    PKGS="python3.8 python3.9 python3.9-venv python3.8-venv"; ($APT_INSTALL $PKGS || apt-get update && $APT_INSTALL $PKGS)) || \
    (curl -O https://www.python.org/ftp/python/3.8.20/Python-3.8.20.tgz && \
    tar -xzf Python-3.8.20.tgz && \
    cd Python-3.8.20 && \
    ./configure --enable-shared --enable-optimizations --prefix=/usr/local LDFLAGS="-Wl,--rpath=/usr/local/lib" && \
    make altinstall && \
    cd .. && \
    curl -O https://www.python.org/ftp/python/3.9.25/Python-3.9.25.tgz && \
    tar -xzf Python-3.9.25.tgz && \
    cd Python-3.9.25 && \
    ./configure --enable-shared --enable-optimizations --prefix=/usr/local LDFLAGS="-Wl,--rpath=/usr/local/lib" && \
    make altinstall)

RUN curl -sS https://bootstrap.pypa.io/pip/3.9/get-pip.py | python3.9

RUN curl -sS https://bootstrap.pypa.io/pip/3.8/get-pip.py | python3.8

RUN echo 'deb https://cloud.r-project.org/bin/linux/ubuntu jammy-cran40/' >> /etc/apt/sources.list
RUN gpg --keyserver hkps://keyserver.ubuntu.com --recv-key E298A3A825C0D65DFD57CBB651716619E084DAB9
RUN gpg -a --export E084DAB9 | apt-key add -
RUN add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu jammy-cran40/'
RUN apt update
RUN $APT_INSTALL r-base
RUN Rscript -e "install.packages(c('remotes', 'knitr', 'markdown', 'rmarkdown', 'testthat', 'e1071', 'survival', 'arrow', 'roxygen2', 'xml2'), repos='https://cloud.r-project.org/')"

# See more in SPARK-39959, roxygen2 < 7.2.1
RUN Rscript -e "install.packages(c('remotes'), repos='https://cloud.r-project.org/')"
RUN Rscript -e "remotes::install_version('roxygen2', version='7.2.0', repos='https://cloud.r-project.org')"

# See more in SPARK-39735
ENV R_LIBS_SITE "/usr/local/lib/R/site-library:${R_LIBS_SITE}:/usr/lib/R/library"

RUN python3.8 -m pip  install --only-binary=pandas numpy pandas 'scipy<1.9' coverage 'matplotlib==3.7.2'
RUN python3.9 -m pip install 'numpy==1.25.1' 'pyarrow==12.0.1' 'pandas<=2.0.3' 'scipy<=1.10' unittest-xml-reporting plotly>=4.8 'mlflow>=2.3.1' coverage 'matplotlib==3.7.2' openpyxl 'memory-profiler==0.60.0' 'scikit-learn==1.1.*' 'blinker==1.4'

# Add Python deps for Spark Connect.
RUN python3.9 -m pip install 'grpcio>=1.48,<1.57' 'grpcio-status>=1.48,<1.57' 'protobuf==3.20.3' 'googleapis-common-protos==1.56.4'

# Add torch as a testing dependency for TorchDistributor
RUN python3.9 -m pip install 'torch==2.0.1' 'torchvision==0.15.2' torcheval
