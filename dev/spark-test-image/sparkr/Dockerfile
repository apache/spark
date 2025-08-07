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
LABEL org.opencontainers.image.ref.name="Apache Spark Infra Image for SparkR"
# Overwrite this label to avoid exposing the underlying Ubuntu OS version label
LABEL org.opencontainers.image.version=""

ENV FULL_REFRESH_DATE=20241114

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
    pandoc \
    pkg-config \
    qpdf \
    r-base \
    software-properties-common \
    wget \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

RUN echo 'deb https://cloud.r-project.org/bin/linux/ubuntu jammy-cran40/' >> /etc/apt/sources.list
RUN gpg --keyserver hkps://keyserver.ubuntu.com --recv-key E298A3A825C0D65DFD57CBB651716619E084DAB9
RUN gpg -a --export E084DAB9 | apt-key add -
RUN add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu jammy-cran40/'

# See more in SPARK-39959, roxygen2 < 7.2.1
RUN Rscript -e "install.packages(c('devtools', 'knitr', 'markdown',  \
    'rmarkdown', 'testthat', 'devtools', 'e1071', 'survival', 'arrow',  \
    'ggplot2', 'mvtnorm', 'statmod', 'xml2'), repos='https://cloud.r-project.org/')" && \
    Rscript -e "devtools::install_version('roxygen2', version='7.2.0', repos='https://cloud.r-project.org')" && \
    Rscript -e "devtools::install_version('lintr', version='2.0.1', repos='https://cloud.r-project.org')" && \
    Rscript -e "devtools::install_version('pkgdown', version='2.0.1', repos='https://cloud.r-project.org')" && \
    Rscript -e "devtools::install_version('preferably', version='0.4', repos='https://cloud.r-project.org')"

# See more in SPARK-39735
ENV R_LIBS_SITE="/usr/local/lib/R/site-library:${R_LIBS_SITE}:/usr/lib/R/library"
