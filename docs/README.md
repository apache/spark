Welcome to the Spark documentation!

This readme will walk you through navigating and building the Spark documentation, which is included here with the Spark source code. You can also find documentation specific to release versions of Spark at http://spark-project.org/documentation.html.

Read on to learn more about viewing documentation in plain text (i.e., markdown) or building the documentation yourself that corresponds to whichever version of Spark you currently have checked out of revision control.

## Generating the Documentation HTML

We include the Spark documentation as part of the source (as opposed to using a hosted wiki as the definitive documentation) to enable the documentation to evolve along with the source code and be captured by revision control (currently git). This way the code automatically includes the version of the documentation that is relevant regardless of which version or release you have checked out or downloaded.

In this directory you will find textfiles formatted using Markdown, with an ".md" suffix. You can read those text files directly if you want. Start with index.md.

To make things quite a bit prettier and make the links easier to follow, generate the html version of the documentation based on the src directory by running `jekyll` in the docs directory (You will need to have Jekyll installed, the easiest way to do this is via a Ruby Gem). This will create a directory called _site which will contain index.html as well as the rest of the compiled files. Read more about Jekyll at https://github.com/mojombo/jekyll/wiki.

## Pygments

We also use pygments (http://pygments.org) for syntax highlighting, so you will also need to install that (it requires Python) by running `sudo easy_install Pygments`.

To mark a block of code in your markdown to be syntax highlighted by jekyll during the compile phase, use the following sytax:

    {% highlight scala %}
    // Your scala code goes here, you can replace scala with many other
    // supported languages too.
    {% endhighlight %}

## Scaladoc

You can build just the Spark scaladoc by running `sbt/sbt doc` from the SPARK_PROJECT_ROOT directory.
