# Plan/Status of ongoing work on DataSource V2 JDBC connector

## Plan
| Work Item                                     | Who's on it | Status |
|-----------------------------------------------| ----------- | ------ |
| Batch write ( append, overwrite, truncate)    | shivsood    | WIP    |
| Streaming write                               | TBD         |        |
| Read path implementation                      | TBD         |        |
| Streaming read                                | TBD         |        |
| ??                                            | TBD         |        |

Status -> WIP ( Work in Progress), ReadyForReview, Done

## Others
- Working branch is https://github.com/shivsood/spark-dsv2
- Intrested in contribution? Add work item and your name against it and party on.

## Issues/Questions/Mentions
- mode(overwrite) - what's the overwrite semantics in V2?
  V1 overwrite will create a table if that does not exist. In V2 it seems that overwrite does not support
  create semantics. overwrite with a non-existing table failed following tables:schema() which returned null
  schema indicating no existing table. If not create scematics, then what's the diffrence between append
  and overwrite without column filters.
- mode(overwrite) - why overwrite results in call to truncate(). Was expecting call to overwrite() instead.
- mode(append) - what's the append semantics in V2.
  V1 append would throw an exception if the df col types are not same as table schema. Is that in v2
  achieved by Table::schema? schema returns source schema (as in db) and framework checks if the source schema is
  compatible with the dataframe type. Tested diffirence is number of cols and framework would raise an exception ,
  but diffrence in data type did not raise any exception.
- Lots of trivial logging. Draft implementation with API understanding as main goal

Update date : 7/22


