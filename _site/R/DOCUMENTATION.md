# SparkR Documentation

SparkR documentation is generated using in-source comments annotated using using
`roxygen2`. After making changes to the documentation, to generate man pages,
you can run the following from an R console in the SparkR home directory

    library(devtools)
    devtools::document(pkg="./pkg", roclets=c("rd"))

You can verify if your changes are good by running

    R CMD check pkg/
