#!/bin/sh

# Create and move to a new directory that can be easily cleaned up
mkdir build_SparkR
cd build_SparkR

## Uninstall R 3.1.1 (This is a really ugly process, the RHEL/Centos R packages need work)
sudo rpm --noscripts -e --allmatches R
sudo rm --force /usr/bin/R*
sudo rm -r --force /usr/include/R
sudo rm -r --force /usr/lib64/R
sudo rm -r --force /usr/share/R
sudo rm -r --force /usr/share/doc/R-3.1.1

## Download, build, and install R 3.1.2
# Install devel packages needed to build R
sudo yum install libXt-devel readline-devel pango-devel libjpeg-turbo-devel libtiff-devel
# Download and expand the R source tarball
wget http://cran.rstudio.com/src/base/R-3/R-3.1.2.tar.gz
tar xzvf R-3.1.2.tar.gz
cd R-3.1.2
# Build R
./configure --enable-R-shlib R_RD4PDF="times,hyper"
make
sudo make install

# Re-configure the R/Java connection
sudo -E /usr/local/bin/R CMD javareconf

# Install additional needed R packages
sudo /usr/local/bin/Rscript -e 'install.packages(c("rJava", "Rserve"), repos = "http://cran.rstudio.com")'

# Install Scala 2.10.4
wget http://www.scala-lang.org/files/archive/scala-2.10.4.tgz
tar xzvf scala-2.10.4.tgz
sudo mkdir /usr/local/share/scala
sudo mv scala-2.10.4/* /usr/local/share/scala
rmdir scala-2.10.4

# Clean-up
#cd ..
#rm -r --force build_SparkR