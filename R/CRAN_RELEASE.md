# SparkR CRAN Release

To release SparkR as a package to CRAN, we would use the `devtools` package. Please work with the
`dev@spark.apache.org` community and R package maintainer on this.

### Release

First, check that the `Version:` field in the `pkg/DESCRIPTION` file is updated. Also, check for stale files not under source control.

Note that while `check-cran.sh` is running `R CMD check`, it is doing so with `--no-manual --no-vignettes`, which skips a few vignettes or PDF checks - therefore it will be preferred to run `R CMD check` on the source package built manually before uploading a release.

To upload a release, we would need to update the `cran-comments.md`. This should generally contain the results from running the `check-cran.sh` script along with comments on status of all `WARNING` (should not be any) or `NOTE`. As a part of `check-cran.sh` and the release process, the vignettes is build - make sure `SPARK_HOME` is set and Spark jars are accessible.

Once everything is in place, run in R under the `SPARK_HOME/R` directory:

```R
paths <- .libPaths(); .libPaths(c("lib", paths)); Sys.setenv(SPARK_HOME=tools::file_path_as_absolute("..")); devtools::release(); .libPaths(paths)
```

For more information please refer to http://r-pkgs.had.co.nz/release.html#release-check

### Testing: build package manually

To build package manually such as to inspect the resulting `.tar.gz` file content, we would also use the `devtools` package.

Source package is what get released to CRAN. CRAN would then build platform-specific binary packages from the source package.

#### Build source package

To build source package locally without releasing to CRAN, run in R under the `SPARK_HOME/R` directory:

```R
paths <- .libPaths(); .libPaths(c("lib", paths)); Sys.setenv(SPARK_HOME=tools::file_path_as_absolute("..")); devtools::build("pkg"); .libPaths(paths)
```

(http://r-pkgs.had.co.nz/vignettes.html#vignette-workflow-2)

Similarly, the source package is also created by `check-cran.sh` with `R CMD build pkg`.

For example, this should be the content of the source package:

```sh
DESCRIPTION	R		inst		tests
NAMESPACE	build		man		vignettes

inst/doc/
sparkr-vignettes.html
sparkr-vignettes.Rmd
sparkr-vignettes.Rman

build/
vignette.rds

man/
 *.Rd files...

vignettes/
sparkr-vignettes.Rmd
```

#### Test source package

To install, run this:

```sh
R CMD INSTALL SparkR_2.1.0.tar.gz
```

With "2.1.0" replaced with the version of SparkR.

This command installs SparkR to the default libPaths. Once that is done, you should be able to start R and run:

```R
library(SparkR)
vignette("sparkr-vignettes", package="SparkR")
```

#### Build binary package

To build binary package locally, run in R under the `SPARK_HOME/R` directory:

```R
paths <- .libPaths(); .libPaths(c("lib", paths)); Sys.setenv(SPARK_HOME=tools::file_path_as_absolute("..")); devtools::build("pkg", binary = TRUE); .libPaths(paths)
```

For example, this should be the content of the binary package:

```sh
DESCRIPTION	Meta		R		html		tests
INDEX		NAMESPACE	help		profile		worker
```
