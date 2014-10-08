TODO
-----
#### UI
* Tree view: remove dummy root node
* Graph view add tooltip
* Backfill wizard
* Fix datepicker

#### Command line
* Add support for including upstream and downstream
#### Write unittests
* For each existing operator
#### More Operators!
* HIVE
* BaseDataTransferOperator
* File2MySqlOperator
* PythonOperator
* DagTaskSensor for cross dag dependencies
* PIG
#### Macros
* Hive latest partition
* Previous execution timestamp
* ...
#### Backend
* LocalExecutor, ctrl-c should result in error state instead of forever running
* CeleryExecutor
* Clear should kill running jobs
#### Misc
* Require and use $FLUX_HOME
* Rename core to flux
* BaseJob
    * DagBackfillJob
    * TaskIntanceJob
    * ClearJob?
* Write an hypervisor, looks for dead jobs without a heartbeat and kills
* Authentication with Flask-Login and Flask-Principal
* email_on_retry

#### Testing required
* result queue with remove descendants

#### Wishlist
* Jobs can send pickles of local version to remote executors?
* Support for cron like synthax (0 * * * ) using croniter library
