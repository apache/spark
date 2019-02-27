---
layout: global
title: ORC Files
displayTitle: ORC Files
---

Since Spark 2.3, Spark supports a vectorized ORC reader with a new ORC file format for ORC files.
To do that, the following configurations are newly added. The vectorized reader is used for the
native ORC tables (e.g., the ones created using the clause `USING ORC`) when `spark.sql.orc.impl`
is set to `native` and `spark.sql.orc.enableVectorizedReader` is set to `true`. For the Hive ORC
serde tables (e.g., the ones created using the clause `USING HIVE OPTIONS (fileFormat 'ORC')`),
the vectorized reader is used when `spark.sql.hive.convertMetastoreOrc` is also set to `true`.

<table class="table">
  <tr><th><b>Property Name</b></th><th><b>Default</b></th><th><b>Meaning</b></th></tr>
  <tr>
    <td><code>spark.sql.orc.impl</code></td>
    <td><code>native</code></td>
    <td>The name of ORC implementation. It can be one of <code>native</code> and <code>hive</code>. <code>native</code> means the native ORC support that is built on Apache ORC 1.4. `hive` means the ORC library in Hive 1.2.1.</td>
  </tr>
  <tr>
    <td><code>spark.sql.orc.enableVectorizedReader</code></td>
    <td><code>true</code></td>
    <td>Enables vectorized orc decoding in <code>native</code> implementation. If <code>false</code>, a new non-vectorized ORC reader is used in <code>native</code> implementation. For <code>hive</code> implementation, this is ignored.</td>
  </tr>
</table>
