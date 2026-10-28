# Nested ML class-loading fixtures

Both directories contain an RFormulaModel with the resolved formula y ~ x
and a PipelineModel containing one nested stage.

The allowed-model directory contains a VectorAssembler and is the positive
control. In rejected-model, only the nested stage metadata's class name differs:
it names org.apache.spark.sql.connect.ml.UnregisteredModel, a harmless,
ML-readable/writable class defined in MLClassLoadingSuite.scala. That model
is available on the test classpath but deliberately omitted from the
ServiceLoader registrations. Its metadata can be loaded outside Spark Connect,
while the Connect loader must reject it with the allowlist error.

The metadata is stored as plain JSON text parts. The model data is one Parquet
row with schema label: string, terms: array<array<string>>, hasIntercept: boolean
and values ("y", [["x"]], true). The two Parquet files are identical.
No model training, executable payload, or metadata rewriting is needed at test time.
