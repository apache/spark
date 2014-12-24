#!/bin/sh

# Download RStudio
if [ ! -f rstudio-0.98.1091-x86_64.rpm ]; then
  wget  http://download1.rstudio.org/rstudio-0.98.1091-x86_64.rpm
fi

# Install using the rpm via yum

sudo yum install rstudio-0.98.1091-x86_64.rpm

rm rstudio-0.98.1091-x86_64.rpm

# Add SparkR directory to .libPaths() in order to import SparkR into an Rstudio session

cat >> $HOME/.Rprofile <<EOT
lib_path <- .libPaths()

lib_path <- c(lib_path,"/home/cloudera/SparkR-pkg/lib")

.libPaths(lib_path)

rm(lib_path)
EOT

