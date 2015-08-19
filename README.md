# Shopify's Apache Spark

Spark is a fast and general cluster computing system for Big Data.

This is Shopify's clone with specific to Shopify customizations, mostly
surrounding configuration.

## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the [project web page](http://spark.apache.org/documentation.html)
and [project wiki](https://cwiki.apache.org/confluence/display/SPARK).
This README file only contains basic setup instructions.

## Building Shopify Spark

You can build Shopify spark using `script/setup`, or continuously and incrementally using `script/watch`

## Testing Shopify Spark

To test a Shopify spark build, assemble the spark jar with `script/setup` or maven, and then unset the `spark.yarn.jar` property from the defaults.conf or the config of the application you are using. Spark will then upload your local assembly to your YARN application's staging, no deploy involved.

## Deploying Shopify Spark

The cap deploy script is only for deploying Shopify Spark to production. To deploy, execute `bundle exec cap production deploy`
