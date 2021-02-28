# R on Spark

SparkR is an R package that provides a light-weight frontend to use Spark from R.

### Installing sparkR

Libraries of sparkR need to be created in `$SPARK_HOME/R/lib`. This can be done by running the script `$SPARK_HOME/R/install-dev.sh`.
By default the above script uses the system wide installation of R. However, this can be changed to any user installed location of R by setting the environment variable `R_HOME` the full path of the base directory where R is installed, before running install-dev.sh script.
Example:
```bash
# where /home/username/R is where R is installed and /home/username/R/bin contains the files R and RScript
export R_HOME=/home/username/R
./install-dev.sh
```

### SparkR development

#### Build Spark

Build Spark with [Maven](https://spark.apache.org/docs/latest/building-spark.html#buildmvn) and include the `-Psparkr` profile to build the R package. For example to use the default Hadoop versions you can run

```bash
./build/mvn -DskipTests -Psparkr package
```

#### Running sparkR

You can start using SparkR by launching the SparkR shell with

    ./bin/sparkR

The `sparkR` script automatically creates a SparkContext with Spark by default in
local mode. To specify the Spark master of a cluster for the automatically created
SparkContext, you can run

    ./bin/sparkR --master "local[2]"

To set other options like driver memory, executor memory etc. you can pass in the [spark-submit](https://spark.apache.org/docs/latest/submitting-applications.html) arguments to `./bin/sparkR`

#### Using SparkR from RStudio

If you wish to use SparkR from RStudio, please refer [SparkR documentation](https://spark.apache.org/docs/latest/sparkr.html#starting-up-from-rstudio).

#### Making changes to SparkR

The [instructions](https://spark.apache.org/contributing.html) for making contributions to Spark also apply to SparkR.
If you only make R file changes (i.e. no Scala changes) then you can just re-install the R package using `R/install-dev.sh` and test your changes.
Once you have made your changes, please include unit tests for them and run existing unit tests using the `R/run-tests.sh` script as described below.

#### Generating documentation

The SparkR documentation (Rd files and HTML files) are not a part of the source repository. To generate them you can run the script `R/create-docs.sh`. This script uses `devtools` and `knitr` to generate the docs and these packages need to be installed on the machine before using the script. Also, you may need to install these [prerequisites](https://github.com/apache/spark/tree/master/docs#prerequisites). See also, `R/DOCUMENTATION.md`

### Examples, Unit tests

SparkR comes with several sample programs in the `examples/src/main/r` directory.
To run one of them, use `./bin/spark-submit <filename> <args>`. For example:
```bash
./bin/spark-submit examples/src/main/r/dataframe.R
```
You can run R unit tests by following the instructions under [Running R Tests](https://spark.apache.org/docs/latest/building-spark.html#running-r-tests).

### Running on YARN

The `./bin/spark-submit` can also be used to submit jobs to YARN clusters. You will need to set YARN conf dir before doing so. For example on CDH you can run
```bash
export YARN_CONF_DIR=/etc/hadoop/conf
./bin/spark-submit --master yarn examples/src/main/r/dataframe.R
```
