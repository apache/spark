# syntax = edrevo/dockerfile-plus

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

INCLUDE+ ./dev/spark-test-image/base/Dockerfile
LABEL org.opencontainers.image.ref.name="Apache Spark Infra Image for Linter"

ENV FULL_REFRESH_DATE=20241112

INCLUDE+ ./dev/spark-test-image/base/Dockerfile.apt-get
RUN rm -rf /var/lib/apt/lists/*

RUN Rscript -e "install.packages(c('devtools', 'knitr', 'markdown', 'rmarkdown', 'testthat'), repos='https://cloud.r-project.org/')" \
    && Rscript -e "devtools::install_version('pkgdown', version='2.0.1', repos='https://cloud.r-project.org')" \
    && Rscript -e "devtools::install_version('preferably', version='0.4', repos='https://cloud.r-project.org')" \
    && Rscript -e "devtools::install_version('lintr', version='2.0.1', repos='https://cloud.r-project.org')" \

# See more in SPARK-39735
ENV R_LIBS_SITE="/usr/local/lib/R/site-library:${R_LIBS_SITE}:/usr/lib/R/library"

# Install Python 3.9
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update && apt-get install -y python3.9 python3.9-distutils \
    && rm -rf /var/lib/apt/lists/*
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.9

RUN python3.9 -m pip install \
    'black==23.9.1' \
    'flake8==3.9.0' \
    'googleapis-common-protos-stubs==2.2.0' \
    'grpc-stubs==1.24.11' \
    'grpcio-status==1.67.0' \
    'grpcio==1.67.0' \
    'ipython' \
    'ipython_genutils' \
    'jinja2' \
    'matplotlib' \
    'mypy==1.8.0' \
    'numpy==2.0.2' \
    'numpydoc' \
    'pandas' \
    'pandas-stubs==1.2.0.53' \
    'plotly>=4.8' \
    'pyarrow>=18.0.0' \
    'pytest-mypy-plugins==1.9.3' \
    'pytest==7.1.3' \
    && python3.9 -m pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu \
    && python3.9 -m pip install torcheval \
    && python3.9 -m pip cache purge
