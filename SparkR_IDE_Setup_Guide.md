Setting up an IDE on the SparkR Development VM
===

While the SparkR shell is great for testing and interactive analysis, you may find that you'd prefer an IDE once you start to develop more complex Spark applications.  While there are several possible solutions to this problem, my go-to IDE for all things R is [Rstudio] so I decided to go ahead and install Rstudio on the [SparkR Development VM].  In addition, I wanted to be able to import the SparkR library just like I would any other previously installed R package, so I had to create a `.Rprofile` and update R's default `.libpaths()` file so that it would automatically look for the SparkR-pkg directory.

To make things easier, I've written a small [shell script] that will handle all of this for you (assuming you're working with the Development VM).  To run the shell script, simply execute the following code from the VM's default command line:

```sh
> wget https://raw.githubusercontent.com/cafreeman/SparkR-pkg/master/Spark_IDE_Setup.sh

> sh Spark_IDE_Setup.sh
```

This shell script will download and install Rstudio and also configure R to include SparkR in the list of importable packages by default.

From here, you can fire up Rstudio (either type `rstudio` in the command prompt or look in Applications > Programming) and import SparkR as you would any other package:

```R
> library(SparkR)
```

By importing SparkR through the IDE session, you'll be able to initialize a SparkContext and execute SparkR commands just like you would in the SparkR shell while taking full advantage of the convenience of developing in an IDE.


[Rstudio]: http://www.rstudio.com/

[shell script]: https://raw.githubusercontent.com/cafreeman/SparkR-pkg/master/Spark_IDE_Setup.sh

[SparkR Development VM]: http://adventures.putler.org/blog/2014/12/08/Setting-Up-a-Virtual-Machine-with-SparkR/
