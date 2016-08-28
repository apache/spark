Untitled
================

Overview
--------

SparkR is an R package that provides a light-weight frontend to use Apache Spark from R. In Spark 2.0.0, SparkR provides a distributed data frame implementation that supports data processing operations like selection, filtering, aggregation etc. and distributed machine learning using MLlib.

Getting Started
---------------

We start with an example running on the local machine and provide an overview of SparkR in multiple dimensions: data ingestion, data processing and machine learning.

First, let's load and attach the package.

``` r
library(SparkR)
```

To use SparkR, you need an Apache Spark package where backend codes to be called are compiled and packaged. You may download it from [Apache Spark Website](http://spark.apache.org/downloads.html). Alternatively, we provide an easy-to-use function `install.spark` to complete this process.

``` r
install.spark(overwrite = TRUE)
```

If you have a Spark package, you don't have to install again, but an environment variable should be set to let SparkR know where it is. If you have run the `install.spark` function, this has already been done for you.

``` r
Sys.setenv(SPARK_HOME = "/HOME/spark")
```

`SparkSession` is the entry point into SparkR which connects your R program to a Spark cluster. You can create a `SparkSession` using `sparkR.session` and pass in options such as the application name, any spark packages depended on, etc. We use default settings.

``` r
sparkR.session()
```

    ## Launching java with spark-submit command /Users/junyangq/spark//bin/spark-submit   sparkr-shell /var/folders/jh/6pw_r0d51317krg8ftgy53f40000gn/T//RtmpT7vIHb/backend_portb8c54afe73fa

    ## Java ref type org.apache.spark.sql.SparkSession id 1

The operations in SparkR are centered around a class of R object called `SparkDataFrame`. It is a distributed collection of data organized into named columns, which is conceptually equivalent to a table in a relational database or a data frame in R, but with richer optimizations under the hood.

`SparkDataFrame` can be constructed from a wide array of sources such as: structured data files, tables in Hive, external databases, or existing local R data frames. For example, we create a `SparkDataFrame` from a local R data frame,

``` r
cars <- cbind(model = rownames(mtcars), mtcars)
carsDF <- createDataFrame(cars)
```

We can view the first few rows of the `SparkDataFrame` by `showDF` or `head` function.

``` r
showDF(carsDF)
```

    ## +-------------------+----+---+-----+-----+----+-----+-----+---+---+----+----+
    ## |              model| mpg|cyl| disp|   hp|drat|   wt| qsec| vs| am|gear|carb|
    ## +-------------------+----+---+-----+-----+----+-----+-----+---+---+----+----+
    ## |          Mazda RX4|21.0|6.0|160.0|110.0| 3.9| 2.62|16.46|0.0|1.0| 4.0| 4.0|
    ## |      Mazda RX4 Wag|21.0|6.0|160.0|110.0| 3.9|2.875|17.02|0.0|1.0| 4.0| 4.0|
    ## |         Datsun 710|22.8|4.0|108.0| 93.0|3.85| 2.32|18.61|1.0|1.0| 4.0| 1.0|
    ## |     Hornet 4 Drive|21.4|6.0|258.0|110.0|3.08|3.215|19.44|1.0|0.0| 3.0| 1.0|
    ## |  Hornet Sportabout|18.7|8.0|360.0|175.0|3.15| 3.44|17.02|0.0|0.0| 3.0| 2.0|
    ## |            Valiant|18.1|6.0|225.0|105.0|2.76| 3.46|20.22|1.0|0.0| 3.0| 1.0|
    ## |         Duster 360|14.3|8.0|360.0|245.0|3.21| 3.57|15.84|0.0|0.0| 3.0| 4.0|
    ## |          Merc 240D|24.4|4.0|146.7| 62.0|3.69| 3.19| 20.0|1.0|0.0| 4.0| 2.0|
    ## |           Merc 230|22.8|4.0|140.8| 95.0|3.92| 3.15| 22.9|1.0|0.0| 4.0| 2.0|
    ## |           Merc 280|19.2|6.0|167.6|123.0|3.92| 3.44| 18.3|1.0|0.0| 4.0| 4.0|
    ## |          Merc 280C|17.8|6.0|167.6|123.0|3.92| 3.44| 18.9|1.0|0.0| 4.0| 4.0|
    ## |         Merc 450SE|16.4|8.0|275.8|180.0|3.07| 4.07| 17.4|0.0|0.0| 3.0| 3.0|
    ## |         Merc 450SL|17.3|8.0|275.8|180.0|3.07| 3.73| 17.6|0.0|0.0| 3.0| 3.0|
    ## |        Merc 450SLC|15.2|8.0|275.8|180.0|3.07| 3.78| 18.0|0.0|0.0| 3.0| 3.0|
    ## | Cadillac Fleetwood|10.4|8.0|472.0|205.0|2.93| 5.25|17.98|0.0|0.0| 3.0| 4.0|
    ## |Lincoln Continental|10.4|8.0|460.0|215.0| 3.0|5.424|17.82|0.0|0.0| 3.0| 4.0|
    ## |  Chrysler Imperial|14.7|8.0|440.0|230.0|3.23|5.345|17.42|0.0|0.0| 3.0| 4.0|
    ## |           Fiat 128|32.4|4.0| 78.7| 66.0|4.08|  2.2|19.47|1.0|1.0| 4.0| 1.0|
    ## |        Honda Civic|30.4|4.0| 75.7| 52.0|4.93|1.615|18.52|1.0|1.0| 4.0| 2.0|
    ## |     Toyota Corolla|33.9|4.0| 71.1| 65.0|4.22|1.835| 19.9|1.0|1.0| 4.0| 1.0|
    ## +-------------------+----+---+-----+-----+----+-----+-----+---+---+----+----+
    ## only showing top 20 rows

We use `magrittr` package to chain operations when necessary in the rest of the document.

``` r
library(magrittr)
```

Common data processing operations such as `filter`, `select` are supported on the `SparkDataFrame`.

``` r
carsSubDF <- select(carsDF, "model", "mpg", "hp")
carsSubDF <- filter(carsSubDF, carsSubDF$hp >= 200)
showDF(carsSubDF)
```

    ## +-------------------+----+-----+
    ## |              model| mpg|   hp|
    ## +-------------------+----+-----+
    ## |         Duster 360|14.3|245.0|
    ## | Cadillac Fleetwood|10.4|205.0|
    ## |Lincoln Continental|10.4|215.0|
    ## |  Chrysler Imperial|14.7|230.0|
    ## |         Camaro Z28|13.3|245.0|
    ## |     Ford Pantera L|15.8|264.0|
    ## |      Maserati Bora|15.0|335.0|
    ## +-------------------+----+-----+

SparkR support a number of commonly used functions to aggregate data after grouping.

``` r
carsGPDF <- summarize(groupBy(carsDF, carsDF$gear), count = n(carsDF$gear))
showDF(carsGPDF)
```

    ## +----+-----+
    ## |gear|count|
    ## +----+-----+
    ## | 4.0|   12|
    ## | 3.0|   15|
    ## | 5.0|    5|
    ## +----+-----+

SparkR supports a number of widely used machine learning algorithms. Under the hood, SparkR uses MLlib to train the model. Users can call `summary` to print a summary of the fitted model, `predict` to make predictions on new data, and `write.ml`/`read.ml` to save/load fitted models.

SparkR supports a subset of R formula operators for model fitting, including ‘~’, ‘.’, ‘:’, ‘+’, and ‘-‘. We use linear regression as an example.

``` r
fit <- spark.glm(carsDF, mpg ~ wt + cyl)
```

``` r
summary(fit)
```

    ## 
    ## Deviance Residuals: 
    ## (Note: These are approximate quantiles with relative error <= 0.01)
    ##     Min       1Q   Median       3Q      Max  
    ## -4.2893  -1.7085  -0.4713   1.5729   6.1004  
    ## 
    ## Coefficients:
    ##              Estimate  Std. Error  t value  Pr(>|t|)  
    ## (Intercept)  39.686    1.715       23.141   0         
    ## wt           -3.191    0.75691     -4.2158  0.00022202
    ## cyl          -1.5078   0.41469     -3.636   0.0010643 
    ## 
    ## (Dispersion parameter for gaussian family taken to be 6.592137)
    ## 
    ##     Null deviance: 1126.05  on 31  degrees of freedom
    ## Residual deviance:  191.17  on 29  degrees of freedom
    ## AIC: 156
    ## 
    ## Number of Fisher Scoring iterations: 1

``` r
sparkR.session.stop()
```

Vignettes are long form documentation commonly included in packages. Because they are part of the distribution of the package, they need to be as compact as possible. The `html_vignette` output type provides a custom style sheet (and tweaks some options) to ensure that the resulting html is as small as possible. The `html_vignette` format:

-   Never uses retina figures
-   Has a smaller default figure size
-   Uses a custom CSS stylesheet instead of the default Twitter Bootstrap style

Vignette Info
-------------

Note the various macros within the `vignette` section of the metadata block above. These are required in order to instruct R how to build the vignette. Note that you should change the `title` field and the `\VignetteIndexEntry` to match the title of your vignette.

Styles
------

The `html_vignette` template includes a basic CSS theme. To override this theme you can specify your own CSS in the document metadata as follows:

    output: 
      rmarkdown::html_vignette:
        css: mystyles.css

Figures
-------

The figure sizes have been customised so that you can easily put two images side-by-side.

``` r
plot(1:10)
plot(10:1)
```

![](sparkr-vignettes_files/figure-markdown_github/unnamed-chunk-14-1.png)![](sparkr-vignettes_files/figure-markdown_github/unnamed-chunk-14-2.png)

You can enable figure captions by `fig_caption: yes` in YAML:

    output:
      rmarkdown::html_vignette:
        fig_caption: yes

Then you can use the chunk option `fig.cap = "Your figure caption."` in **knitr**.

More Examples
-------------

You can write math expressions, e.g. *Y* = *X**β* + *ϵ*, footnotes[1], and tables, e.g. using `knitr::kable()`.

|                   |   mpg|  cyl|   disp|   hp|  drat|     wt|   qsec|   vs|   am|  gear|  carb|
|-------------------|-----:|----:|------:|----:|-----:|------:|------:|----:|----:|-----:|-----:|
| Mazda RX4         |  21.0|    6|  160.0|  110|  3.90|  2.620|  16.46|    0|    1|     4|     4|
| Mazda RX4 Wag     |  21.0|    6|  160.0|  110|  3.90|  2.875|  17.02|    0|    1|     4|     4|
| Datsun 710        |  22.8|    4|  108.0|   93|  3.85|  2.320|  18.61|    1|    1|     4|     1|
| Hornet 4 Drive    |  21.4|    6|  258.0|  110|  3.08|  3.215|  19.44|    1|    0|     3|     1|
| Hornet Sportabout |  18.7|    8|  360.0|  175|  3.15|  3.440|  17.02|    0|    0|     3|     2|
| Valiant           |  18.1|    6|  225.0|  105|  2.76|  3.460|  20.22|    1|    0|     3|     1|
| Duster 360        |  14.3|    8|  360.0|  245|  3.21|  3.570|  15.84|    0|    0|     3|     4|
| Merc 240D         |  24.4|    4|  146.7|   62|  3.69|  3.190|  20.00|    1|    0|     4|     2|
| Merc 230          |  22.8|    4|  140.8|   95|  3.92|  3.150|  22.90|    1|    0|     4|     2|
| Merc 280          |  19.2|    6|  167.6|  123|  3.92|  3.440|  18.30|    1|    0|     4|     4|

Also a quote using `>`:

> "He who gives up \[code\] safety for \[code\] speed deserves neither." ([via](https://twitter.com/hadleywickham/status/504368538874703872))

[1] A footnote here.
