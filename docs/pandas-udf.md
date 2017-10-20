# 1. Use only `pandas_udf`
The main issues with this approach as a few people comment out is that it is hard to know what the udf does without look at the implementation.
For instance, for a udf:
```
@pandas_udf(DoubleType())
def foo(v):
      ...
```
It's hard to tell whether this function is a reduction that returns a scalar double, or a transform function that returns a pd.Series of double.

This is less than ideal because:
* The user of the udf cannot tell which functions this udf can be used with. i.e, can this be used with `groupby().apply()` or `withColumn` or `groupby().agg()`?
* Catalyst cannot do validation at planning phase, i.e., it cannot throw exception if user passes a transformation function rather than aggregation function to `groupby().agg()`

# 2. Use different decorators. i,e, `pandas_udf` (or `pandas_scalar_udf`), `pandas_grouped_udf`, `pandas_udaf`

The idea of this approach is to use `pandas_grouped_udf` for all group udfs, and `pandas_scalar_udf` for scalar pandas udfs that gets used with "withColumn". This helps with distinguish between some scalar udf and group udfs. However, this approach doesn't help to distinguish among group udfs. For instance, the group transform and group aggregation examples above.

# 3. Use `pandas_udf` decorate and a function type enum for "one-step" vectorized udf and `pandas_udaf` for multi-step aggregation function

This approach uses a function type enum to describe what the udf does. Here are the proposed function types:
## scalar_transform
A pd.Series(s) -> pd.Series transformation that is independent of the grouping. This is the existing scalar pandas udf.
```
@pandas_udf(DoubleType(), SCALAR_TRANSFORM):
def plus_one(v):
      return v + 1
```
### `scalar_transform` can be used with `withColumn`, `select` and etc:
```
df = df.withColumn("v", plus_one(v))
```
## group_transform
A pd.Series(s) -> pd.Series transformation that is dependent of the grouping. e.g.
```
@pandas_udf(DoubleType(), GROUP_TRANSFORM):
def rank(v):
      return v.rank()
```
### `group_transform` can be used with:

#### window
```
window = Window.partitionBy('date')

df = df.withColumn('rank', rank(df.v).over(w))
```
#### groupby

or **maybe something like this in the future** (Not available with the current API):
```
df = df.withColumn('rank', df.groupby('id').v.transform(rank))
```
for reference, in pandas you would write sth like this:
```
df = df.assign(rank=df.groupby('id').v.transform(lambda v: v.rank()))
```

### although it's also a Series -> Series transformation, `group_transform` will also be rejected by `withColumn`, `select`, etc
```
# This doesn't make sense and will throw exception
df.withColumn(rank(df.v))
```

## group_aggregate:
A pd.Series(s) -> scalar function, e.g.
```
@pandas_udf(DoubleType(), GROUP_AGGREGATE):
def mean(v):
      return v.mean()
```
### can be used with:
#### window
```
window = Window.partitionBy('date')

df = df.withColumn('mean', mean(df.v).over(w))
```

#### groupby
```
df = df.groupby('id').agg(mean(df.v))
```

## group_map (maybe a better name):
This defines a pd.DataFrame -> pd.DataFrame transformation. This is the current `groupby().apply()` udf

```
@pandas_udf(df.schema, GROUP_MAP):
def foo(pdf):
      pdf = pdf.assign(v1 = df.v1 - df.v1.mean())
      pdf = pdf.assign(v2 = df.v2 / df.v2.std())
      return pdf
```
### Can be used with groupby apply:
```
df.groupby('date').apply(foo)
```
