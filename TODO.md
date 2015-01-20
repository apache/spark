TODO
-----
#### UI
* Pause button for dags (affect master scheduler)
* Tree view: remove dummy root node
* Backfill wizard

#### Write unittests
* For each existing operator

#### Command line
* `airflow task_state dag_id task_id YYYY-MM-DD`

#### More Operators!
* S3Sensor
* BaseDataTransferOperator
* File2MySqlOperator
* PythonOperator
* DagTaskSensor for cross dag dependencies
* PIG

#### Macros
* Previous execution timestamp
* ...

#### Backend
* Add decorator to timeout imports on master process [lib](https://github.com/pnpnpn/timeout-decorator)
* Clear should kill running jobs
* Mysql port should carry through (using default now)
* Make authentication universal

#### Misc
* Write an hypervisor, looks for dead jobs without a heartbeat and kills

#### Wishlist
* Support for cron like synthax (0 * * * ) using croniter library
