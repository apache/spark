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

# Image for building Spark releases. Based on Ubuntu 22.04.
FROM ubuntu:jammy-20240227
LABEL org.opencontainers.image.authors="Apache Spark project <dev@spark.apache.org>"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.ref.name="Apache Spark Release Manager Image"
# Overwrite this label to avoid exposing the underlying Ubuntu OS version label
LABEL org.opencontainers.image.version=""

ENV FULL_REFRESH_DATE 20240318

ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN true

RUN apt-get update && apt-get install -y \
    build-essential \
    ca-certificates \
    curl \
    gfortran \
    git \
    subversion \
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
    nodejs \
    npm \
    openjdk-17-jdk-headless \
    pandoc \
    pkg-config \
    python3.10 \
    python3-psutil \
    texlive-latex-base \
    texlive \
    texlive-fonts-extra \
    texinfo \
    texlive-latex-extra \
    qpdf \
    jq \
    r-base \
    ruby \
    ruby-dev \
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
ENV R_LIBS_SITE "/usr/local/lib/R/site-library:${R_LIBS_SITE}:/usr/lib/R/library"


RUN add-apt-repository ppa:pypy/ppa
RUN mkdir -p /usr/local/pypy/pypy3.9 && \
    curl -sqL https://downloads.python.org/pypy/pypy3.9-v7.3.16-linux64.tar.bz2 | tar xjf - -C /usr/local/pypy/pypy3.9 --strip-components=1 && \
    ln -sf /usr/local/pypy/pypy3.9/bin/pypy /usr/local/bin/pypy3.9 && \
    ln -sf /usr/local/pypy/pypy3.9/bin/pypy /usr/local/bin/pypy3
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | pypy3
RUN pypy3 -m pip install numpy 'six==1.16.0' 'pandas==2.2.2' scipy coverage matplotlib lxml


ARG BASIC_PIP_PKGS="numpy pyarrow>=15.0.0 six==1.16.0 pandas==2.2.2 scipy plotly>=4.8 mlflow>=2.8.1 coverage matplotlib openpyxl memory-profiler>=0.61.0 scikit-learn>=1.3.2 twine==3.4.1"
# Python deps for Spark Connect
ARG CONNECT_PIP_PKGS="grpcio==1.62.0 grpcio-status==1.62.0 protobuf==4.25.1 googleapis-common-protos==1.56.4"

# Install Python 3.10 packages
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10
RUN python3.10 -m pip install --ignore-installed blinker>=1.6.2 # mlflow needs this
RUN python3.10 -m pip install --ignore-installed 'six==1.16.0'  # Avoid `python3-six` installation
RUN python3.10 -m pip install $BASIC_PIP_PKGS unittest-xml-reporting $CONNECT_PIP_PKGS && \
    python3.10 -m pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu && \
    python3.10 -m pip install deepspeed torcheval && \
    python3.10 -m pip cache purge

# Install Python 3.9
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update && apt-get install -y \
    python3.9 python3.9-distutils \
    && rm -rf /var/lib/apt/lists/*
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.9
RUN python3.9 -m pip install --ignore-installed blinker>=1.6.2 # mlflow needs this
RUN python3.9 -m pip install --force $BASIC_PIP_PKGS unittest-xml-reporting $CONNECT_PIP_PKGS && \
    python3.9 -m pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu && \
    python3.9 -m pip install torcheval && \
    python3.9 -m pip cache purge

# Should unpin 'sphinxcontrib-*' after upgrading sphinx>5
# See 'ipython_genutils' in SPARK-38517
# See 'docutils<0.18.0' in SPARK-39421
RUN python3.9 -m pip install 'sphinx==4.5.0' mkdocs 'pydata_sphinx_theme>=0.13' sphinx-copybutton nbsphinx numpydoc jinja2 markupsafe 'pyzmq<24.0.0' \
ipython ipython_genutils sphinx_plotly_directive 'numpy>=1.20.0' pyarrow pandas 'plotly>=4.8' 'docutils<0.18.0' \
'flake8==3.9.0' 'mypy==1.8.0' 'pytest==7.1.3' 'pytest-mypy-plugins==1.9.3' 'black==23.9.1' \
'pandas-stubs==1.2.0.53' 'grpcio==1.62.0' 'grpc-stubs==1.24.11' 'googleapis-common-protos-stubs==2.2.0' \
'sphinxcontrib-applehelp==1.0.4' 'sphinxcontrib-devhelp==1.0.2' 'sphinxcontrib-htmlhelp==2.0.1' 'sphinxcontrib-qthelp==1.0.3' 'sphinxcontrib-serializinghtml==1.1.5'
RUN python3.9 -m pip list

RUN gem install --no-document "bundler:2.4.22"
RUN ln -s "$(which python3.9)" "/usr/local/bin/python"
RUN ln -s "$(which python3.9)" "/usr/local/bin/python3"

WORKDIR /opt/spark-rm/output

ARG UID
RUN useradd -m -s /bin/bash -p spark-rm -u $UID spark-rm
USER spark-rm:spark-rm

ENTRYPOINT [ "/opt/spark-rm/do-release.sh" ]
