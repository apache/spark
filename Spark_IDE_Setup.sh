
# Download RStudio

wget  http://download1.rstudio.org/rstudio-0.98.1091-x86_64.rpm

# Install using the rpm via yum

sudo yum install rstudio-0.98.1091-x86_64.rpm

{echo 'lib_path <- .libPaths()' ; echo 'lib_path <- c(lib_path,"/home/cloudera/SparkR-pkg/lib")'; echo '.libPaths(lib_path)'; } >> ~/.Rprofile

cat <<EOT >> .Rprofile
lib_path <- .libPaths()

lib_path <- c(lib_path,"/home/cloudera/SparkR-pkg/lib")

.libPaths(lib_path)

rm(lib_path)
EOT