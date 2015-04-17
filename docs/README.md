Welcome to the Spark documentation!

This readme will walk you through navigating and building the Spark documentation, which is included
here with the Spark source code. You can also find documentation specific to release versions of
Spark at http://spark.apache.org/documentation.html.

Read on to learn more about viewing documentation in plain text (i.e., markdown) or building the
documentation yourself. Why build it yourself? So that you have the docs that corresponds to
whichever version of Spark you currently have checked out of revision control.

## Generating the Documentation HTML

We include the Spark documentation as part of the source (as opposed to using a hosted wiki, such as
the github wiki, as the definitive documentation) to enable the documentation to evolve along with
the source code and be captured by revision control (currently git). This way the code automatically
includes the version of the documentation that is relevant regardless of which version or release
you have checked out or downloaded.

In this directory you will find textfiles formatted using Markdown, with an ".md" suffix. You can
read those text files directly if you want. Start with index.md.

The markdown code can be compiled to HTML using the [Jekyll tool](http://jekyllrb.com).
`Jekyll` and a few dependencies must be installed for this to work. We recommend
installing via the Ruby Gem dependency manager. Since the exact HTML output
varies between versions of Jekyll and its dependencies, we list specific versions here
in some cases:

    $ sudo gem install jekyll
    $ sudo gem install jekyll-redirect-from

Execute `jekyll` from the `docs/` directory. Compiling the site with Jekyll will create a directory
called `_site` containing index.html as well as the rest of the compiled files.

You can modify the default Jekyll build as follows:

    # Skip generating API docs (which takes a while)
    $ SKIP_API=1 jekyll build
    # Serve content locally on port 4000
    $ jekyll serve --watch
    # Build the site with extra features used on the live page
    $ PRODUCTION=1 jekyll build

## Pygments

We also use pygments (http://pygments.org) for syntax highlighting in documentation markdown pages,
so you will also need to install that (it requires Python) by running `sudo pip install Pygments`.

To mark a block of code in your markdown to be syntax highlighted by jekyll during the compile
phase, use the following sytax:

    {% highlight scala %}
    // Your scala code goes here, you can replace scala with many other
    // supported languages too.
    {% endhighlight %}

## Sphinx

We use Sphinx to generate Python API docs, so you will need to install it by running
`sudo pip install sphinx`.

## knitr, devtools

SparkR documentation is written using `roxygen2` and we use `knitr`, `devtools` to generate
documentation. To install these packages you can run `install.packages(c("knitr", "devtools"))` from a
R console.

## API Docs (Scaladoc, Sphinx, roxygen2)

You can build just the Spark scaladoc by running `build/sbt unidoc` from the SPARK_PROJECT_ROOT directory.

Similarly, you can build just the PySpark docs by running `make html` from the
SPARK_PROJECT_ROOT/python/docs directory. Documentation is only generated for classes that are listed as
public in `__init__.py`. The SparkR docs can be built by running SPARK_PROJECT_ROOT/R/create-docs.sh.

When you run `jekyll` in the `docs` directory, it will also copy over the scaladoc for the various
Spark subprojects into the `docs` directory (and then also into the `_site` directory). We use a
jekyll plugin to run `build/sbt unidoc` before building the site so if you haven't run it (recently) it
may take some time as it generates all of the scaladoc.  The jekyll plugin also generates the
PySpark docs [Sphinx](http://sphinx-doc.org/).

NOTE: To skip the step of building and copying over the Scala, Python, R API docs, run `SKIP_API=1
jekyll`.
