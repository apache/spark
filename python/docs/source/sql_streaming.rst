.. See also ``pyspark.sql.sources.DataSource.streamReader``.

The parameter `read_limit` in `latestOffset` provides the read limit for the current batch.
The implementation can use this information to cap the number of rows returned in the batch.
For example, if the `read_limit` is `{"maxRows": 1000}`, the data source should not return
more than 1000 rows. The available read limit types are:

* `maxRows`: the maximum number of rows to return in a batch.
* `minRows`: the minimum number of rows to return in a batch.
* `maxBytes`: the maximum size in bytes to return in a batch.
* `minBytes`: the minimum size in bytes to return in a batch.
* `allAvailable`: return all available data in a batch.
