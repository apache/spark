# Spark binary packages -- Affirm version history

Documents every **productive** Affirm release of `spark-<spark-version>-bin-hadoop3-affirm.<N>.tgz`
(published to `generic-local/affirm-spark/`).

| Type | Format | Document here? |
|------|--------|----------------|
| Productive | `1`, `2`, `3`, ... (integer only) | **Yes, required** |
| Development | `dev1`, `dev2`, ... | No, exempt |

For a productive release the build script requires a section `## <spark-version>-affirm.<N>` that
contains a `**Commit SHA:** <full-sha>` line matching the `--commit` argument. Both checks run before
compilation. Published names are permanent (no delete or overwrite), so never reuse a version.

<!-- Add new productive releases below, newest first. -->

## 3.5.1-affirm.2

**Spark version:** 3.5.1
**Commit SHA:** e7ae8e714cd82916d563650b315e70fc9d11f9d2

**Changes:**
- `hadoop-aws` 3.3.4 and `iceberg-spark-runtime-3.5_2.12` 1.6.1 added in `pom.xml` (6 commits above upstream `v3.5.1`).
- Built with Java 8, flags `-Pmesos -Pkubernetes -Pscala-2.12 -Phadoop-3 -Phive -Phive-thriftserver -Denforcer.skip`.
- Earlier packages (`affirm.3` to `affirm.5` and others) were published by hand and are not documented here.
