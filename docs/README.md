Welcome to the Spark documentation!

This readme will walk you through navigating and building the Spark documentation, which is included here with the Spark source code. You can also find documentation specific to release versions of Spark at http://spark.incubator.apache.org/documentation.html.

Read on to learn more about viewing documentation in plain text (i.e., markdown) or building the documentation yourself. Why build it yourself? So that you have the docs that corresponds to whichever version of Spark you currently have checked out of revision control.

## Generating the Documentation HTML

We include the Spark documentation as part of the source (as opposed to using a hosted wiki, such as the github wiki, as the definitive documentation) to enable the documentation to evolve along with the source code and be captured by revision control (currently git). This way the code automatically includes the version of the documentation that is relevant regardless of which version or release you have checked out or downloaded.

In this directory you will find textfiles formatted using Markdown, with an ".md" suffix. You can read those text files directly if you want. Start with index.md.

To make things quite a bit prettier and make the links easier to follow, generate the html version of the documentation based on the src directory by running `jekyll build` in the docs directory. Use the command `SKIP_SCALADOC=1 jekyll build` to skip building and copying over the scaladoc which can be timely. To use the `jekyll` command, you will need to have Jekyll installed, the easiest way to do this is via a Ruby Gem, see the [jekyll installation instructions](http://jekyllrb.com/docs/installation). This will create a directory called _site containing index.html as well as the rest of the compiled files. Read more about Jekyll at https://github.com/mojombo/jekyll/wiki.

In addition to generating the site as html from the markdown files, jekyll can serve up the site via a webserver. To build and run a local webserver use the command `jekyll serve` (or the faster variant `SKIP_SCALADOC=1 jekyll serve`), which runs the webserver on port 4000, then visit the site at http://localhost:4000.

## Pygments

We also use pygments (http://pygments.org) for syntax highlighting in documentation markdown pages, so you will also need to install that (it requires Python) by running `sudo easy_install Pygments`.

To mark a block of code in your markdown to be syntax highlighted by jekyll during the compile phase, use the following sytax:

    {% highlight scala %}
    // Your scala code goes here, you can replace scala with many other
    // supported languages too.
    {% endhighlight %}

## API Docs (Scaladoc and Epydoc)

You can build just the Spark scaladoc by running `sbt/sbt doc` from the SPARK_PROJECT_ROOT directory.

Similarly, you can build just the PySpark epydoc by running `epydoc --config epydoc.conf` from the SPARK_PROJECT_ROOT/pyspark directory.

When you run `jekyll` in the docs directory, it will also copy over the scaladoc for the various Spark subprojects into the docs directory (and then also into the _site directory). We use a jekyll plugin to run `sbt/sbt doc` before building the site so if you haven't run it (recently) it may take some time as it generates all of the scaladoc.  The jekyll plugin also generates the PySpark docs using [epydoc](http://epydoc.sourceforge.net/).

NOTE: To skip the step of building and copying over the Scala and Python API docs, run `SKIP_API=1 jekyll`.
