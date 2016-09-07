---
layout: global
title: Submitting Applications
---

The `spark-submit` script in Spark's `bin` directory is used to launch applications on a cluster.
It can use all of Spark's supported [cluster managers](cluster-overview.html#cluster-manager-types)
through a uniform interface so you don't have to configure your application specially for each one.

# Bundling Your Application's Dependencies
If your code depends on other projects, you will need to package them alongside
your application in order to distribute the code to a Spark cluster. To do this,
create an assembly jar (or "uber" jar) containing your code and its dependencies. Both
[sbt](https://github.com/sbt/sbt-assembly) and
[Maven](http://maven.apache.org/plugins/maven-shade-plugin/)
have assembly plugins. When creating assembly jars, list Spark and Hadoop
as `provided` dependencies; these need not be bundled since they are provided by
the cluster manager at runtime. Once you have an assembled jar you can call the `bin/spark-submit`
script as shown here while passing your jar.

For Python, you can use the `--py-files` argument of `spark-submit` to add `.py`, `.zip` or `.egg`
files to be distributed with your application. If you depend on multiple Python files we recommend
packaging them into a `.zip` or `.egg`. For a more complex packaging system for Python, see the
section *Advanced Dependency Management* bellow)

# Launching Applications with spark-submit

Once a user application is bundled, it can be launched using the `bin/spark-submit` script.
This script takes care of setting up the classpath with Spark and its
dependencies, and can support different cluster managers and deploy modes that Spark supports:

{% highlight bash %}
./bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]
{% endhighlight %}

Some of the commonly used options are:

* `--class`: The entry point for your application (e.g. `org.apache.spark.examples.SparkPi`)
* `--master`: The [master URL](#master-urls) for the cluster (e.g. `spark://23.195.26.187:7077`)
* `--deploy-mode`: Whether to deploy your driver on the worker nodes (`cluster`) or locally as an external client (`client`) (default: `client`) <b> &#8224; </b>
* `--conf`: Arbitrary Spark configuration property in key=value format. For values that contain spaces wrap "key=value" in quotes (as shown).
* `application-jar`: Path to a bundled jar including your application and all dependencies. The URL must be globally visible inside of your cluster, for instance, an `hdfs://` path or a `file://` path that is present on all nodes.
* `application-arguments`: Arguments passed to the main method of your main class, if any

<b>&#8224;</b> A common deployment strategy is to submit your application from a gateway machine
that is
physically co-located with your worker machines (e.g. Master node in a standalone EC2 cluster).
In this setup, `client` mode is appropriate. In `client` mode, the driver is launched directly
within the `spark-submit` process which acts as a *client* to the cluster. The input and
output of the application is attached to the console. Thus, this mode is especially suitable
for applications that involve the REPL (e.g. Spark shell).

Alternatively, if your application is submitted from a machine far from the worker machines (e.g.
locally on your laptop), it is common to use `cluster` mode to minimize network latency between
the drivers and the executors. Currently, standalone mode does not support cluster mode for Python
applications.

For Python applications, simply pass a `.py` file in the place of `<application-jar>` instead of a JAR,
and add Python `.zip`, `.egg` or `.py` files to the search path with `--py-files`.

There are a few options available that are specific to the
[cluster manager](cluster-overview.html#cluster-manager-types) that is being used.
For example, with a [Spark standalone cluster](spark-standalone.html) with `cluster` deploy mode,
you can also specify `--supervise` to make sure that the driver is automatically restarted if it
fails with non-zero exit code. To enumerate all such options available to `spark-submit`,
run it with `--help`. Here are a few examples of common options:

{% highlight bash %}
# Run application locally on 8 cores
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master local[8] \
  /path/to/examples.jar \
  100

# Run on a Spark standalone cluster in client deploy mode
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master spark://207.184.161.138:7077 \
  --executor-memory 20G \
  --total-executor-cores 100 \
  /path/to/examples.jar \
  1000

# Run on a Spark standalone cluster in cluster deploy mode with supervise
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master spark://207.184.161.138:7077 \
  --deploy-mode cluster \
  --supervise \
  --executor-memory 20G \
  --total-executor-cores 100 \
  /path/to/examples.jar \
  1000

# Run on a YARN cluster
export HADOOP_CONF_DIR=XXX
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master yarn \
  --deploy-mode cluster \  # can be client for client mode
  --executor-memory 20G \
  --num-executors 50 \
  /path/to/examples.jar \
  1000

# Run a Python application on a Spark standalone cluster
./bin/spark-submit \
  --master spark://207.184.161.138:7077 \
  examples/src/main/python/pi.py \
  1000

# Run on a Mesos cluster in cluster deploy mode with supervise
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master mesos://207.184.161.138:7077 \
  --deploy-mode cluster \
  --supervise \
  --executor-memory 20G \
  --total-executor-cores 100 \
  http://path/to/examples.jar \
  1000

{% endhighlight %}

# Master URLs

The master URL passed to Spark can be in one of the following formats:

<table class="table">
<tr><th>Master URL</th><th>Meaning</th></tr>
<tr><td> <code>local</code> </td><td> Run Spark locally with one worker thread (i.e. no parallelism at all). </td></tr>
<tr><td> <code>local[K]</code> </td><td> Run Spark locally with K worker threads (ideally, set this to the number of cores on your machine). </td></tr>
<tr><td> <code>local[*]</code> </td><td> Run Spark locally with as many worker threads as logical cores on your machine.</td></tr>
<tr><td> <code>spark://HOST:PORT</code> </td><td> Connect to the given <a href="spark-standalone.html">Spark standalone
        cluster</a> master. The port must be whichever one your master is configured to use, which is 7077 by default.
</td></tr>
<tr><td> <code>mesos://HOST:PORT</code> </td><td> Connect to the given <a href="running-on-mesos.html">Mesos</a> cluster.
        The port must be whichever one your is configured to use, which is 5050 by default.
        Or, for a Mesos cluster using ZooKeeper, use <code>mesos://zk://...</code>.
        To submit with <code>--deploy-mode cluster</code>, the HOST:PORT should be configured to connect to the <a href="running-on-mesos.html#cluster-mode">MesosClusterDispatcher</a>.
</td></tr>
<tr><td> <code>yarn</code> </td><td> Connect to a <a href="running-on-yarn.html"> YARN </a> cluster in
        <code>client</code> or <code>cluster</code> mode depending on the value of <code>--deploy-mode</code>.
        The cluster location will be found based on the <code>HADOOP_CONF_DIR</code> or <code>YARN_CONF_DIR</code> variable.
</td></tr>
</table>


# Loading Configuration from a File

The `spark-submit` script can load default [Spark configuration values](configuration.html) from a
properties file and pass them on to your application. By default it will read options
from `conf/spark-defaults.conf` in the Spark directory. For more detail, see the section on
[loading default configurations](configuration.html#loading-default-configurations).

Loading default Spark configurations this way can obviate the need for certain flags to
`spark-submit`. For instance, if the `spark.master` property is set, you can safely omit the
`--master` flag from `spark-submit`. In general, configuration values explicitly set on a
`SparkConf` take the highest precedence, then flags passed to `spark-submit`, then values in the
defaults file.

If you are ever unclear where configuration options are coming from, you can print out fine-grained
debugging information by running `spark-submit` with the `--verbose` option.

# Advanced Dependency Management
When using `spark-submit`, the application jar along with any jars included with the `--jars` option
will be automatically transferred to the cluster. URLs supplied after `--jars` must be separated by commas. That list is included on the driver and executor classpaths. Directory expansion does not work with `--jars`.

Spark uses the following URL scheme to allow different strategies for disseminating jars:

- **file:** - Absolute paths and `file:/` URIs are served by the driver's HTTP file server, and
  every executor pulls the file from the driver HTTP server.
- **hdfs:**, **http:**, **https:**, **ftp:** - these pull down files and JARs from the URI as expected
- **local:** - a URI starting with local:/ is expected to exist as a local file on each worker node.  This
  means that no network IO will be incurred, and works well for large files/JARs that are pushed to each worker,
  or shared via NFS, GlusterFS, etc.

Note that JARs and files are copied to the working directory for each SparkContext on the executor nodes.
This can use up a significant amount of space over time and will need to be cleaned up. With YARN, cleanup
is handled automatically, and with Spark standalone, automatic cleanup can be configured with the
`spark.worker.cleanup.appDataTtl` property.

Users may also include any other dependencies by supplying a comma-delimited list of maven coordinates
with `--packages`. All transitive dependencies will be handled when using this command. Additional
repositories (or resolvers in SBT) can be added in a comma-delimited fashion with the flag `--repositories`.
These commands can be used with `pyspark`, `spark-shell`, and `spark-submit` to include Spark Packages.

# Wheel Support for Pyspark

For Python, the `--py-files` option can be used to distribute `.egg`, `.zip`, `.whl` and `.py`
libraries to executors. This modigies the `PYTHONPATH` environment variable to inject this
dependency into the executed script environment.

This solution does not scale well with complex project with multiple dependencies.

There are however other solutions to deploy projects with dependencies:
- Describe dependencies inside a `requirements.txt` and have everything installed into an isolated
  virtual environment
- Distribute a Python Source Distribution Package or a Wheel with a complete Python module

Dependency distribution is also configurable:
- Let each node of the Spark Cluster automatically fetch and install dependencies from Pypi or any
  configured Pypi mirror
- Distribute a single archive *wheelhouse* with all dependencies precompiled in Wheels files

**What is a Wheel?**

  A Wheel is a Python packaging format that contains a fully prepared module for a given system or
  environment. Wheel also allow to be installed on several system. For example, modules such as
  Numpy requires compilation of some C files, so the Wheels allows to store the object files already
  compiled inside the various wheel packages. That why wheel might be system dependent (ex: 32
  bits/64 bits, Linux/Windows/Mac OS, ...). The Wheel format also provides a unified metadata
  description, so tools such as `pip install` will automatically select the precompiled wheel
  package that best fit the current system.

  Said differently, if the wheel is already prepared, no compilation occurs during installation.

**How to deploy Wheels or Wheelhouse**

The usage of the deployment methods described in the current section of this documentation is
recommended for the following cases:

- your PySpark script has increased in complexity and dependencies. For example, it now depends on
  numpy for a numerical calculus, and a bunch extra packages from Pypi you are used to work with
- you project might also depends on package that are *not* on Pypi, for example, Python libraries
  internal to your company
- you do not want to deal with the IT department each time you need a new Python package on each
  Spark node. For example, you need an upgraded version of a module A
- you have conflict with some version of dependencies, you need a certain version of a given
  package, while another team wants another version of the same package

All deployment methods described bellow involve the creation of a temporary virtual environment,
using `virtualenv`, on each node of the Spark Cluster. This will be automatically done during the
`spark-submit` step.

Please also be aware than Wheelhouse support with a virtualenv is only supported for YARN and
standalone cluster. Mesos cluster does not support Wheelhouse yet. You can however send wheel with
`--py-files` to inject whl in the `PYTHON_PATH`.

The following methods only differe by the way Wheels files are sent or retrieved in your Spark
cluster nodes:

- use *Big Fat Wheelhouse* when all node of your Spark Cluster does not have access to the Internet
  and so cannot hit `pypi.python.org` website, and if your do not have any Pypi mirror internal to
  your organization.
- if your Spark cluster have access to the official Pypi or a mirror, you can configure
  `spark-submit` so `pip` will automatically find and download the dependency wheels.
- You can use `--py-files` to send all wheels manually.

**Pypi Mirror**

Mirroring might be useful for company with a strict Internet access policy, or weird Proxy settings.

There are several solutions for mirroring Pypi internally to your organization:
- http://doc.devpi.net/latest/: a Free mirroring solution
- Artifactory provides
  [Pypi mirroring](https://www.jfrog.com/confluence/display/RTF/PyPI+Repositories) in its
  non-commercial license

**Project packaging Workflow**

The following workflow describes how to deploy a complex Python project with different dependencies
to your Spark Cluster, for example, you are writing a PySpark job that depends on a library that is
only available from inside your organization, or you use a special version of Pypi package such as
Numpy.

The workflow is now:

- create a virtualenv in a directory, for instance `env`, if not already in one:

{% highlight bash %}
virtualenv env
{% endhighlight %}

- Turn your script into a real standard Python package. You need to write a standard `setup.py` to
  make your package installable through `pip install -e . -r requirements.txt` (do not use `python
  setup.py develop` or `python setup.py install` with `pip`, [it is not
  recommended](https://pip.pypa.io/en/stable/reference/pip_install/#hash-checking-mode) and does not
  handle editable dependencies correctly).

- Write a `requirements.txt`. It is recommended to freeze the version of each dependency in this
  description file. This way your job will be garentee to be deployable everytime, even if a buggy
  version of a dependency is released on Pypi.

  Example of `requirements.txt` with frozen package version:

      astroid==1.4.6
      autopep8==1.2.4
      click==6.6
      colorama==0.3.7
      enum34==1.1.6
      findspark==1.0.0
      first==2.0.1
      hypothesis==3.4.0
      lazy-object-proxy==1.2.2
      linecache2==1.0.0
      pbr==1.10.0
      pep8==1.7.0
      pip-tools==1.6.5
      py==1.4.31
      pyflakes==1.2.3
      pylint==1.5.6
      pytest==2.9.2
      six==1.10.0
      spark-testing-base==0.0.7.post2
      traceback2==1.4.0
      unittest2==1.1.0
      wheel==0.29.0
      wrapt==1.10.8

  Ensure you write a clean `setup.py` that refers this `requirements.txt` file:

      from pip.req import parse_requirements

      # parse_requirements() returns generator of pip.req.InstallRequirement objects
      install_reqs = parse_requirements(<requirements_path>, session=False)

      # reqs is a list of requirement
      # e.g. ['django==1.5.1', 'mezzanine==1.4.6']
      reqs = [str(ir.req) for ir in install_reqs]

      setup(
      ...
          install_requires=reqs
      )

- If you want to deploy a *Big Fat Wheelhouse* archive, ie, a zip file containing all Wheels:

  Create the wheelhouse for your current project:
{% highlight bash %}
pip install wheelhouse
pip wheel . --wheel-dir wheelhouse -r requirements.txt
{% endhighlight %}

  Note: please use Pip >= 8.1.3 to generate all wheels, even for "editable" dependencies. To specify
  reference to other internal project, you can use the following syntax in your `requirements.txt`

      -e git+ssh://local.server/a/project/name@7f4a7623aa219743e9b96b228b4cd86fe9bc5595#egg=package_name

  It is highly recommended to specify the SHA-1, a tag or a branch of the internal dependency, and
  update it at each release. Do not forget to specify the `egg` package name.

  Documentation: [Requirements Files](https://pip.pypa.io/en/latest/user_guide/#requirements-files)

  Usually, most dependencies will not be compiled if it has been done previously, `pip` is able to
  cache all wheel file be default in the user home cache, and if wheels can be found from Pypi they
  are automatically retrieved.

  At the end you have all the `.whl` required *for your current system* in the `wheelhouse`
  directory.

  Zip the {{wheelhouse}} directory into a {{wheelhouse.zip}}.

{% highlight bash %}
rm -fv wheelhouse.zip
zip -vrj wheelhouse.zip wheelhouse
{% endhighlight %}

  You now have a `wheelhouse.zip` archive with all the dependencies of your project *and also your
  project module*. For example, if you have defined a module `mymodule` in `setup.py`, it will be
  automatically installed with `spark-submit`.

- If you want to let Spark cluster access to Pypi or a Pypi mirror, just build the source
  distribution package:

      python setup.py sdist

  Or the wheel with:

      python setup.py bdist_wheel

  You now have a tar.gz or a whl file containing your current project.

  Note: most of the time, your Spark job will not have low-level dependency, so building a source
  distribution package is enough.

To execute your application, you need a *launcher script*, ie, a script which is executed directly
and will call your built package. There is no equivalent to the `--class` argument of `spark-submit`
for Python jobs. Please note that this file might only contain a few lines. For example:

{% highlight python %}
/#!/usr/bin/env python

from mypackage import run
run()
{% endhighlight %}

Note that all the logic is stored into the `mypackage` module, which has been declared in you
`setup.py`.

**Deployment Modes support**

In **Standalone**, only the `client` deployment mode is supported. You cannot use 'cluster'
deployment. This means the driver will be executed from the machine that execute the `spark-submit`.
So you **need** to execute the `spark-submit` from within your development virtualenv.

Workers will perform installation of dedicated virtualenv, if `spark.pyspark.virtualenv.enabled` is
set to `True`.

In **YARN**, if you use `client` deployment mode, you also need to execute the `spark-submit` from
your virtualenv. You use `cluster` deployment, the virtualenv installation will be performed like on
all workers.

There is no support for **Mesos** cluster with Virtualenv. Note than you can send wheel file with
`--py-files` and they will be added to `PYTHON_PATH`. This is not recommended since you will not
benefit from the advantages of installing with `pip`:

- you cannot have `pip` automatically retrieve missing Python dependencies from `pypi.python.org`
- you cannot prepare and send several version of the same package, to support different
  architectures (ex: some worker uses python 3.4, others python 3.4, or some machines are 32 bits
  or other are 64 bits, or some are under MacOS X and other are under Linux,...)

To have these advantages, you can use the Wheelhouse deployment described bellow.

**Submitting your package to the Spark Cluster:**

Please remember that in "standalone" Spark instance, only the `client` deployment mode is supported.

Deploy a script with many dependencies to your standalone cluster:

{% highlight bash %}
source /path/to/your/project/env/bin/activate # ensure you are in virtualenv
bin/spark-submit
  --master spark://localhost
  --deploy-mode client
  --jars java-dependencies.jar
  --files /path/to/your/project/requirements.txt
  --conf "spark.pyspark.virtualenv.enabled=true"
  --conf "spark.pyspark.virtualenv.requirements=requirements.txt"
  --conf "spark.pyspark.virtualenv.index_url=https://pypi.python.org/simple"
  /path/to/launch/simple_script_with_some_dependencies.py
{% endhighlight %}

Execution:
- a virtualenv is created on each worker
- the dependencies described in `requirements.txt` are installed in each worker.
- dependencies are downloaded from the Pypi repository
- the driver is executed on the client, so this command line should be executed from *within* a
  virtualenv.


Deploy a simple runner script along with a source distribution package of the complete job project:

{% highlight bash %}
source /path/to/your/project/env/bin/activate # ensure you are in virtualenv
bin/spark-submit
  --master spark://localhost
  --deploy-mode client
  --jars some-java-dependencies
  --files /path/to/mypackage_sdist.tag.gz
  --conf "spark.pyspark.virtualenv.enabled=true"
  --conf "spark.pyspark.virtualenv.install_package=mypackage_sdist.tar.gz"
  --conf "spark.pyspark.virtualenv.index_url=https://pypi.python.org/simple"
  /path/to/launch/runner_script.py
{% endhighlight %}

Execution:
- a virtualenv is created on each worker
- the package `mypackage_sdist.tar.gz` is installed with pip, so if the `setup.py`` refers
  `requirements.txt` properly, each the dependencies are installed in each worker.
- dependencies are downloaded from the Pypi repository
- the driver is executed on the client, so this command line should be executed from *within* a
  virtualenv.
- the runner script simply call an entry point within `mypackage`.


Deploy a wheelhouse package to your YARN cluster with:

{% highlight bash %}
bin/spark-submit
  --master yarn
  --deploy-mode cluster
  --jars java-dependencies.jar
  --files /path/to/your/project/requirements.txt,/path/to/your/project/wheelhouse.zip
  --conf "spark.pyspark.virtualenv.enabled=true"
  --conf "spark.pyspark.virtualenv.requirements=requirements.txt"
  --conf "spark.pyspark.virtualenv.install_package=a_package.whl"
  --conf "spark.pyspark.virtualenv.index_url=https://pypi.python.org/simple"
  /path/to/launch/launcher_script.py
{% endhighlight %}

Execution:
- a virtualenv is created on each worker
- the dependencies described in `requirements.txt` are installed in each worker
- dependencies are found into the wheelhouse archive. If not found, it will be downloaded from Pypi
  repository (to avoid this, remove `spark.pyspark.virtualenv.index_url` option)
- the driver is executed on the cluster, so this command line does *not* have to be executed from
  within a virtualenv.


To deploy against an internal Pypi mirror (HTTPS mirror without certificates), force pip
upgrade (it is a good practice to always be at the latest version of pip), and inject some wheels
manually to the `PYTHONPATH`:

{% highlight bash %}
bin/spark-submit
  --master yarn
  --deploy-mode cluster
  --jars java-dependencies.jar
  --files /path/to/your/project/requirements.txt
  --py-files /path/to/your/project/binary/myproject.whl,/path/to/internal/dependency/other_project.whl
  --conf "spark.pyspark.virtualenv.enabled=true"
  --conf "spark.pyspark.virtualenv.requirements=requirements.txt"
  --conf "spark.pyspark.virtualenv.upgrade_pip=true"
  --conf "spark.pyspark.virtualenv.index_url=https://pypi.mycompany.com/"`
  --conf "spark.pyspark.virtualenv.trusted_host=pypi.mycompany.com"
  /path/to/launch/script.py
{% endhighlight %}

Execution:
- a virtualenv is created on each worker
- the pip tool is updated to the latest version
- the dependencies described in `requirements.txt` are installed in each worker
- dependencies are found into the wheelhouse archive. If not found, it will be downloaded from a Pypi
  mirror
- the two wheels set in the `--py-files` are added to the `PYTHONPATH`. You can use this to avoid
  describing them in the `requirements.txt` and send them directly. Might be useful for development,
  however for production you might want to have these dependency projects available on an internal
  repository and referenced by URL.
- the driver is executed on the cluster, so this command line does *not* have to be executed from
  within a virtualenv.


Here are the description of the configuration of the support of Wheel and Wheelhouse in Python:

- `--jars java-dependencies.jar`: you still need to define the Java jars your requires inside a big
  fat jar file with this argument, for instance if you use Spark Streaming.
- `spark.pyspark.virtualenv.enabled`: enable the creation of the virtualenv environment at each
  deployment and trigger the installation of wheels. This virtual environment creation has a time
  and disk space cost. Please note that, when deploying a Big Fat Wheelhouse, *no network*
  connection to pypi.python.org or any mirror will be made.
- `--files /path/to/your/project/requirements.txt,/path/to/your/project/wheelhouse.zip`: this will
  simply copy these two files to the root of the job working directory on each node. Enabling
  'virtualenv' will automatically use these files when they are found. Having at least
  `requirements.txt` is mandatory.
- `--conf spark.pyspark.virtualenv.type=conda`: you can specify the format of your requirements.txt.
  This parameter is optional. The default value, `native`, will use the native `pip` tool to install
  your package on each Spark node. You can also use `conda` for Conda package manager.
- `--conf spark.pyspark.virtualenv.requirements=other_requirement.txt`: specify the name of the
  requirement file. This parameter is optional. The default value is `requirements.txt`. Do not
  forget to copy this file to the cluster with `--files` argument.
- `--conf spark.pyspark.virtualenv.bin.path=venv`: specify the command to create the virtual env.
  This parameter is optional. The default value, `virtualenv`, should work on every kind of system,
  but if you need to specify a different command line name (ex: `venv` for Python 3) or specify a
  full path, set this value.
- `--conf spark.pyspark.virtualenv.wheelhouse=mywheelhouse.zip`: name of the wheelhouse archive.
  This parameter is optional. The default value is `wheelhouse.zip`. Do not forget to move this file
  to the cluster with `--files` argument. If found, the file will be unzipped to the `wheelhouse`
  directory. It is not mandatory to use this archive to transfer modules found on Pypi if you have
  Internet connectivity or a mirror of Pypi reachable from each worker. Use it primarily for
  transfering precompiled, internal module dependencies.
- `--conf spark.pyspark.virtualenv.upgrade_pip=true`: upgrade `pip` automatically. It is a good
  behavior to always have the latest `pip` version. Default: `false`.
- `--conf spark.pyspark.virtualenv.index_url=http://internalserver/pypimirror`: change the Pypi
  repository URL (Default: `https://pypi.python.org/simple`, requires a network connectivity)
- `--conf spark.pyspark.virtualenv.trusted_host=internalserver`: Execute `pip` with the
  `--trusted-host` argument, ie, provide the name of the server hostname to trusted, even though it
  does not have valid or any HTTPS. Useful when using a Pypi mirror behind HTTPS without a full
  certificate chain.
- `--conf spark.pyspark.virtualenv.system_site_packages=true`: this makes virtual environment
  reference also the packages installed on the system. The default value, `false` will force
  developers to specify all dependencies and let `pip` install them from `requirements.txt`. Set the
  value to `true` to use preinstalled packages on each node. A virtualenv will still be created so
  installing new packages will not compromise the worker Python installation.
- `--conf spark.pyspark.virtualenv.use_index=false`: if set to `false`, don't try to download
  missing dependencies from Pypi or the index URL set by `spark.pyspark.virtualenv.index_url`, in
  which case all dependencies should be packaged in the `wheelhouse.zip` archive. Default is set to
  `true`. Please note that if `spark.pyspark.virtualenv.index_url` is manually set,
  `spark.pyspark.virtualenv.use_index` will be forced to `true`.
- `--py-files /path/to/a/project/aproject.whl,/path/to/internal/dependency/other_project.whl`:
  this allows to copy wheel to the cluster nodes and install them with `pip`. Using this arguments
  implies two things:
    - all wheels will be installed, you cannot have one wheel for linux 32 bits and another one for
      linux 64 bits. In this situation zip them into a single archive and use `--files
      wheelhouse.zip`
    - you need to create the wheel of other internal dependencies (ie that are not on Pypi) manually
      or select them after having made a `pip wheel`
- `/path/to/launch/script.py`: path to the runner script. Like said earlier, it is recommended to
  keep this file as short as possible, and only call a `run()`-like method from a package defined in
  your `setup.py`.

**Advantages**

- Installation is fast and does not require compilation
- No Internet connectivity needed when using a Big Fat Wheelhouse, no need to mess with your
  corporate proxy, or even require a local mirroring of pypi.
- Package versions are isolated, so two Spark job can depends on two different version of a given
  library without any conflict
- wheels are automatically cached (for pip version > 7.0), at the worst case, only the first time it
  is downloaded the compilation might take time. Please note that compilation is quite rare since
  most of the time the package on Pypi already provides precompiled wheels for major Python version
  and systems (Ex: look all the wheels provided by [Numpy](https://pypi.python.org/pypi/numpy)).

**Disadvantages**

- Creating a virtualenv at each execution takes time, not that much, but still it can take some
  seconds
- And consume more disk space than a simpler script without any dependency
- This is slighly more complex to setup than sending a simple python script
- The support of heterogenous Spark nodes (ex: Linux 32 bits/64 bits,...) is possible but you need
  to ensure **all** wheels are in the wheelhouse, to ensure pip is able to install all needed
  package on each node of you Spark cluster. The complexity of this task, that might be not trivial,
  is moved on the hands of the script developer and not on the IT department


**Configuration Pypi proxy**

To tell `spark-submit` to use a Pypi mirror internal to your company, you can use
`--conf "spark.pyspark.virtualenv.index_url=http://pypi.mycompany.com/"` argument.

You can also update the {{~/.pip/pip.conf}} file of each node of your Spark cluster to point by
default to your mirror:

{% highlight ini %}
[global]
; Low timeout
timeout = 20
index-url = https://&lt;user&gt;:&lt;pass&gt;@pypi.mycompany.org/
{% endhighlight %}

Note: pip does not use system certificates, if you need to set up on manually, add this line in the
`[global]` section of `pip.conf`:

{% highlight ini %}
cert = /path/to/your/internal/certificates.pem
{% endhighlight %}

# More Information

Once you have deployed your application, the [cluster mode overview](cluster-overview.html) describes
the components involved in distributed execution, and how to monitor and debug applications.
