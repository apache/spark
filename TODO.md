TODO
-----
#### UI
* Backfill wizard

#### unittests
* Increase coverage, now 85ish%

#### Command line
* `airflow task_state dag_id task_id YYYY-MM-DD`

#### More Operators!
* BaseDataTransferOperator
* File2MySqlOperator
* PIG

#### Backend
* Make authentication universal
* Callbacks
* Master auto dag refresh at time intervals
* Prevent timezone chagne on import
* Add decorator to timeout imports on master process [lib](https://github.com/pnpnpn/timeout-decorator)

#### Wishlist
* Support for cron like synthax (0 * * * ) using croniter library
* Pause flag at the task level
