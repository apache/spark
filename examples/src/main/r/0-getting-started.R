#
# Author:   Daniel Emaasit (@emaasit)
# Purpose: This script shows how to install SparkR onto your workstation/PC
#          and initialize a spark context and a SparkSQL context
# Date:    06/04/2015
#


# Install SparkR from CRAN
install.packages("SparkR")

## OR Install the dev version from Github
install.packages(devtools)
devtools::install_github("amplab-extras/SparkR-pkg", subdir="pkg")

# Load SparkR onto your PC
library(SparkR)

## Initialize SparkContext on your local PC
sc <- sparkR.init(master = "local", appName = "MyApp")

## Initialize SQLContext
sqlCtx <- SparkRSQL.init(sc)