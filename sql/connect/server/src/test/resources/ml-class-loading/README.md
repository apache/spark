# Nested ML class-loading fixtures

Both directories contain an RFormulaModel with the resolved formula y ~ x
and a PipelineModel containing one VectorAssembler stage.

The allowed-model directory is the positive control. In rejected-model, only
the nested stage metadata's class name differs: it names
org.apache.spark.sql.connect.ml.UnregisteredTestStage, an inert placeholder
that is deliberately absent from the classpath and the ML allowlist.
The loader must reject that name with an allowlist error before trying to
resolve the class. A missing-class error does not satisfy the test.

The metadata is stored as plain JSON text parts. The model data is one Parquet
row with schema label: string, terms: array<array<string>>, hasIntercept: boolean
and values ("y", [["x"]], true). The two Parquet files are identical.
No model training, executable payload, or metadata rewriting is needed at test time.
