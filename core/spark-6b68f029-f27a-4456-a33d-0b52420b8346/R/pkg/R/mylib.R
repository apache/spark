myfunc <- function(x) {
  SparkR:::callJStatic("a.MyLib", "myFunc", x)
}
      