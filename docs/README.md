---
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

Welcome to the Spark documentation!

This readme will walk you through navigating and building the Spark documentation, which is included
here with the Spark source code. You can also find documentation specific to release versions of
Spark at https://spark.apache.org/documentation.html.

Read on to learn more about viewing documentation in plain text (i.e., markdown) or building the
documentation yourself. Why build it yourself? So that you have the docs that correspond to
whichever version of Spark you currently have checked out of revision control.

## Prerequisites

The Spark documentation build uses a number of tools to build HTML docs and API docs in Scala, Java, Python, R, and SQL.

You need to have [Ruby 3][ruby] and [Python 3][python] installed. Make sure the `bundle` command is available. If not, install it as follows:

[ruby]: https://www.ruby-lang.org/en/documentation/installation/
[python]: https://www.python.org/downloads/

```sh
$ gem install bundler -v 2.4.22
```

After this all the required Ruby dependencies can be installed from the `docs/` directory via Bundler:

```sh
$ cd "$SPARK_HOME"/docs
$ bundle install
```

And the required Python dependencies can be installed using pip:

```sh
$ cd "$SPARK_HOME"
$ pip install --upgrade -r dev/requirements.txt
```

To generate the Python or R API docs, you'll also need to [install Pandoc](https://pandoc.org/installing.html).

### R API Documentation (Optional)

If you'd like to generate R API documentation, install these libraries:

```sh
$ sudo Rscript -e 'install.packages(c("knitr", "devtools", "testthat", "rmarkdown"), repos="https://cloud.r-project.org/")'
$ sudo Rscript -e 'devtools::install_version("roxygen2", version = "7.1.2", repos="https://cloud.r-project.org/")'
$ sudo Rscript -e "devtools::install_version('pkgdown', version='2.0.1', repos='https://cloud.r-project.org')"
$ sudo Rscript -e "devtools::install_version('preferably', version='0.4', repos='https://cloud.r-project.org')"
```

Note: Other versions of roxygen2 might work in SparkR documentation generation but `RoxygenNote` field in `$SPARK_HOME/R/pkg/DESCRIPTION` is 7.1.2, which is updated if the version is mismatched.

## Generating the Documentation HTML

We include the Spark documentation as part of the source (as opposed to using a hosted wiki, such as
the github wiki, as the definitive documentation) to enable the documentation to evolve along with
the source code and be captured by revision control (currently git). This way the code automatically
includes the version of the documentation that is relevant regardless of which version or release
you have checked out or downloaded.

In this directory you will find text files formatted using Markdown, with an ".md" suffix. You can
read those text files directly if you want. Start with `index.md`.

Execute `SKIP_API=1 bundle exec jekyll build` from the `docs/` directory to compile the site. Compiling the site with
Jekyll will create a directory called `_site` containing `index.html` as well as the rest of the
compiled files.

```sh
$ cd docs
# Skip generating API docs (which takes a while)
$ SKIP_API=1 bundle exec jekyll build
```

You can also generate the default Jekyll build with API Docs as follows:

```sh
$ bundle exec jekyll build

# Serve content locally on port 4000
$ bundle exec jekyll serve --watch

# Build the site with extra features used on the live page
$ PRODUCTION=1 bundle exec jekyll build
```

## API Docs (Scaladoc, Javadoc, Sphinx, roxygen2, MkDocs)

You can build just the Spark scaladoc and javadoc by running `./build/sbt unidoc` from the `$SPARK_HOME` directory.

Similarly, you can build just the PySpark docs by running `make html` from the
`$SPARK_HOME/python/docs` directory. Documentation is only generated for classes that are listed as
public in `__init__.py`. The SparkR docs can be built by running `$SPARK_HOME/R/create-docs.sh`, and
the SQL docs can be built by running `$SPARK_HOME/sql/create-docs.sh`
after [building Spark](https://github.com/apache/spark#building-spark) first.

When you run `bundle exec jekyll build` in the `docs` directory, it will also copy over the scaladoc and javadoc for the various
Spark subprojects into the `docs` directory (and then also into the `_site` directory). We use a
jekyll plugin to run `./build/sbt unidoc` before building the site so if you haven't run it (recently) it
may take some time as it generates all of the scaladoc and javadoc using [Unidoc](https://github.com/sbt/sbt-unidoc).
The jekyll plugin also generates the PySpark docs using [Sphinx](http://sphinx-doc.org/), SparkR docs
using [roxygen2](https://cran.r-project.org/web/packages/roxygen2/index.html) and SQL docs
using [MkDocs](https://www.mkdocs.org/).

To control what API docs get built, you can set any combination of the following shell variables before you run `bundle exec jekyll build`:
* `SKIP_API=1`: Skip building all the API docs.
* `SKIP_SCALADOC=1`: Skip the Scala and Java API docs.
* `SKIP_PYTHONDOC=1`: Skip the Python API docs.
* `SKIP_RDOC=1`: Skip the R API docs.
* `SKIP_SQLDOC=1`: Skip the SQL API docs.

