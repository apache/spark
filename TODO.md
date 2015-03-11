TODO
-----
#### UI
* Backfill wizard

#### unittests
* Increase coverage, now 80ish%

#### Command line
* `airflow task_state dag_id task_id YYYY-MM-DD`

#### More Operators!
* Sandbox the BashOperator
* S3Sensor
* BaseDataTransferOperator
* File2MySqlOperator
* DagTaskSensor for cross dag dependencies
* PIG

#### Frontend
*

#### Backend
* Callbacks
* Master auto dag refresh at time intervals
* Prevent timezone chagne on import
* Add decorator to timeout imports on master process [lib](https://github.com/pnpnpn/timeout-decorator)
* Make authentication universal

#### Misc
* Write an hypervisor, looks for dead jobs without a heartbeat and kills

#### Wishlist
* Support for cron like synthax (0 * * * ) using croniter library
