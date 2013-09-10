.onLoad <- function(libname, pkgname) {
  library(rJava)
  sparkR.onLoad(libname, pkgname)
}
