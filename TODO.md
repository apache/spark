TODO
-----
#### UI
* Run button / backfill wizard
* Add templating to adhoc queries
* Charts: better error handling

#### Command line
* Backfill, better logging, prompt with details about what tasks are about to run

#### More Operators!
* PIG
* MR
* Merge Cascading

#### Backend
* Add a run_only_latest flag to BaseOperator, runs only most recent task instance where deps are met
* Pickle all the THINGS!
* Add priority_weight(Int) to BaseOperator, +@property subtree_priority
* Distributed scheduler
* Add decorator to timeout imports on master process [lib](https://github.com/pnpnpn/timeout-decorator)

#### Wishlist
* Support for cron like synthax (0 * * * ) using croniter library
* Pause flag at the task level
* Task callbacks as tasks?
* Increase unit test coverage
