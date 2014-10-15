TODO
-----
#### UI
* Tree view: remove dummy root node
* Backfill wizard
* Fix datepicker

#### Command line
* New ascii art on command line

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
* CeleryExecutor
* Clear should kill running jobs

#### Misc
* Write an hypervisor, looks for dead jobs without a heartbeat and kills
* Authentication with Flask-Login and Flask-Principal
* email_on_retry

#### Wishlist
* Support for cron like synthax (0 * * * ) using croniter library
