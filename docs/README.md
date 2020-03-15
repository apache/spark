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

The Spark documentation build uses a number of tools to build HTML docs and API docs in Scala, Java,
Python, R and SQL.

You need to have Ruby 2 (preferably Ruby 2.6+) and Python 3 (preferably Python 3.7+) installed.

You'll also need to install the following libraries:

```sh
gem install jekyll:4.0.0 jekyll-redirect-from:0.16.0 rouge:3.15.0
```

### Using rbenv and pyenv

A handy way to install and manage various versions of Ruby and Python is with [`rbenv`] and [`pyenv`].

[`rbenv`]: https://github.com/rbenv/rbenv
[`pyenv`]: https://github.com/pyenv/pyenv

On macOS you can install them with Homebrew:

```sh
brew install rbenv pyenv
```

To activate them, you'll need to run these commands or add them to the end of your `.bash_profile`:

```sh
eval "$(rbenv init -)"
eval "$(pyenv init -)"
```

You can now use them to install specific versions of Ruby and Python and associate them with 
the Spark home directory. Whenever you navigate to this directory or any of its subdirectories, these versions of Ruby and Python will be automatically activated.

```sh
rbenv install 2.7.0
pyenv install 3.7.6

cd /path/to/spark/root
rbenv local 2.7.0
pyenv local 3.7.6
```

### R Documentation

If you'd like to generate R documentation, you'll need to install R, [install Pandoc](https://pandoc.org/installing.html),
and install these libraries:

```sh
$ sudo Rscript -e 'install.packages(c("knitr", "devtools", "testthat", "rmarkdown"), repos="https://cloud.r-project.org/")'
$ sudo Rscript -e 'devtools::install_version("roxygen2", version = "5.0.1", repos="https://cloud.r-project.org/")'
```

Note: Other versions of roxygen2 might work in SparkR documentation generation but `RoxygenNote` field in `$SPARK_HOME/R/pkg/DESCRIPTION` is 5.0.1, which is updated if the version is mismatched.

### API Documentation

To generate API docs for any language, you'll need to install these libraries:

```sh
pip install sphinx==2.3.1 mkdocs==1.0.4 numpy==1.18.1
```

## Generating the Documentation HTML

We include the Spark documentation as part of the source (as opposed to using a hosted wiki, such as
the github wiki, as the definitive documentation) to enable the documentation to evolve along with
the source code and be captured by revision control (currently git). This way the code automatically
includes the version of the documentation that is relevant regardless of which version or release
you have checked out or downloaded.

In this directory you will find text files formatted using Markdown, with an ".md" suffix. You can
read those text files directly if you want. Start with `index.md`.

Execute `jekyll build` from the `docs/` directory to compile the site. Compiling the site with
Jekyll will create a directory called `_site` containing `index.html` as well as the rest of the
compiled files.

```sh
$ cd docs
$ jekyll build
```

You can modify the default Jekyll build as follows:

```sh
# Skip generating API docs (which takes a while)
$ SKIP_API=1 jekyll build

# Serve content locally on port 4000
$ jekyll serve --watch

# Build the site with extra features used on the live page
$ PRODUCTION=1 jekyll build
```

## API Docs (Scaladoc, Javadoc, Sphinx, roxygen2, MkDocs)

You can build just the Spark scaladoc and javadoc by running `./build/sbt unidoc` from the `$SPARK_HOME` directory.

Similarly, you can build just the PySpark docs by running `make html` from the
`$SPARK_HOME/python/docs` directory. Documentation is only generated for classes that are listed as
public in `__init__.py`. The SparkR docs can be built by running `$SPARK_HOME/R/create-docs.sh`, and
the SQL docs can be built by running `$SPARK_HOME/sql/create-docs.sh`
after [building Spark](https://github.com/apache/spark#building-spark) first.

When you run `jekyll build` in the `docs` directory, it will also copy over the scaladoc and javadoc for the various
Spark subprojects into the `docs` directory (and then also into the `_site` directory). We use a
jekyll plugin to run `./build/sbt unidoc` before building the site so if you haven't run it (recently) it
may take some time as it generates all of the scaladoc and javadoc using [Unidoc](https://github.com/sbt/sbt-unidoc).
The jekyll plugin also generates the PySpark docs using [Sphinx](http://sphinx-doc.org/), SparkR docs
using [roxygen2](https://cran.r-project.org/web/packages/roxygen2/index.html) and SQL docs
using [MkDocs](https://www.mkdocs.org/).

NOTE: To skip the step of building and copying over the Scala, Java, Python, R and SQL API docs, run `SKIP_API=1
jekyll build`. In addition, `SKIP_SCALADOC=1`, `SKIP_PYTHONDOC=1`, `SKIP_RDOC=1` and `SKIP_SQLDOC=1` can be used
to skip a single step of the corresponding language. `SKIP_SCALADOC` indicates skipping both the Scala and Java docs.

### Automatically Rebuilding API Docs

`jekyll serve --watch` will only watch what's in `docs/`, and it won't follow symlinks. That means it won't monitor your API docs under `python/docs` or elsewhere.

To work around this limitation for Python, install [`entr`](http://eradman.com/entrproject/) and run the following in a separate shell:

```sh
cd "$SPARK_HOME/python/docs"
find .. -type f -name '*.py' \
| entr -s 'make html && cp -r _build/html/. ../../docs/api/python'
```

Whenever there is a change to your Python code, `entr` will automatically rebuild the Python API docs and copy them to `docs/`, thus triggering a Jekyll update.
