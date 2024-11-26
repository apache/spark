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

FROM ubuntu:jammy-20240911.1
LABEL org.opencontainers.image.ref.name="Apache Spark Infra Image for Documentation"

ENV FULL_REFRESH_DATE 20241029

# INCLUDE+ ./dev/spark-test-image/base/Dockerfile
# INCLUDE+ ../../../dev/spark-test-image/base/Dockerfile
# INCLUDE+ ../../dev/spark-test-image/base/Dockerfile
INCLUDE+ ../base/Dockerfile

# Should unpin 'sphinxcontrib-*' after upgrading sphinx>5
# See 'ipython_genutils' in SPARK-38517
# See 'docutils<0.18.0' in SPARK-39421
RUN python3.9 -m pip install 'sphinx==4.5.0' mkdocs 'pydata_sphinx_theme>=0.13' sphinx-copybutton nbsphinx numpydoc jinja2 markupsafe 'pyzmq<24.0.0' \
  ipython ipython_genutils sphinx_plotly_directive 'numpy>=1.20.0' pyarrow pandas 'plotly>=4.8' 'docutils<0.18.0' \
  'flake8==3.9.0' 'mypy==1.8.0' 'pytest==7.1.3' 'pytest-mypy-plugins==1.9.3' 'black==23.9.1' \
  'pandas-stubs==1.2.0.53' 'grpcio==1.62.0' 'grpcio-status==1.62.0' 'grpc-stubs==1.24.11' 'googleapis-common-protos-stubs==2.2.0' \
  'sphinxcontrib-applehelp==1.0.4' 'sphinxcontrib-devhelp==1.0.2' 'sphinxcontrib-htmlhelp==2.0.1' 'sphinxcontrib-qthelp==1.0.3' 'sphinxcontrib-serializinghtml==1.1.5' \
  && python3.9 -m pip cache purge
