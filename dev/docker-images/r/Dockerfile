FROM palantirtechnologies/circle-spark-base

USER root

### Install R
RUN echo "deb http://cran.rstudio.com/bin/linux/ubuntu trusty/" >> /etc/apt/sources.list \
  && apt-get update \
  && gpg --keyserver keyserver.ubuntu.com --recv-key E084DAB9 \
  && gpg -a --export E084DAB9 | sudo apt-key add - \
  && apt-get update \
  && apt-get --assume-yes install r-base r-base-dev qpdf \
  && rm -rf /var/lib/apt/lists/* \
  && chmod 777 /usr/local/lib/R/site-library \
  && /usr/lib/R/bin/R -e "install.packages(c('devtools'), repos='http://cran.us.r-project.org', lib='/usr/local/lib/R/site-library'); devtools::install_github('r-lib/testthat@v1.0.2', lib='/usr/local/lib/R/site-library'); install.packages(c('knitr', 'rmarkdown', 'e1071', 'survival', 'roxygen2', 'lintr'), repos='http://cran.us.r-project.org', lib='/usr/local/lib/R/site-library')"
ENV R_HOME=/usr/lib/R

USER circleci

