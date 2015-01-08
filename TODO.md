TODO
-----
#### UI
* User login / security
* Tree view: remove dummy root node
* Backfill wizard

#### Write unittests
* For each existing operator

#### Command line
* `airflow test dag_id task_id YYYY-MM-DD` (outputs log to stdout, doesnt care about dependencies, states)
* `airflow list_dags`
* `airflow list_tasks dag_idj`
* `airflow task_state dag_id task_id YYYY-MM-DD`

#### More Operators!
* HIVE
* BaseDataTransferOperator
* File2MySqlOperator
* PythonOperator
* DagTaskSensor for cross dag dependencies
* PIG

#### Macros
* Previous execution timestamp
* ...

#### Backend
* CeleryExecutor
* Master to derive BaseJob
* Clear should kill running jobs
* Mysql port should carry through

#### Misc
* Write an hypervisor, looks for dead jobs without a heartbeat and kills
* Authentication with Flask-Login and Flask-Principal
* email_on_retry

#### Wishlist
* Support for cron like synthax (0 * * * ) using croniter library
