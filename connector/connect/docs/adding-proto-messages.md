# Required, Optional and default values

Connect adopts proto3, which does not support `required` constraint anymore.
Every field is optional. For non-message proto fields, there is also no `hasXXX`
functions to easy tell if a filed is set or not-set. For those non-message proto,
they also have default values. E.g. int has 0 as the default value.


### Required field

Even proto3 does not offer `required` constraint, there is still some fields that
are semantically required. For such case, we share add comment on the field to mark
it as required. The expectation for client implementation (or any submitted proto plan)
is that such fields should be always set, and server will always assume such fields
are set and use whatever values of the fields directly. It is the client side's fault
to not offer meaningful value for `required` field and in that case, the behavior on
the server side is not defined.


### Optional field and default value

Optional fields could have default values when the field is not set, and we are seeing
two cases that depends on whether the default value for the proto field is consistent
with the corresponding Spark parameter default value.

If both default values of the proto field and Spark parameter are the same, we keep the
proto field as is.

For example:
```Scala
// Spark Plan.
class FooPlan(size: Long = 0)
```

```protobuf
// Connect proto.
message Foo {
  int64 size; 
}
```

```Scala
// Spark access Connect proto.
foo = new proto.Foo()
// If size in Foo is set, then get the value.
// If size in Foo is not set, this will take the default proto value,
// which matches with Spark parameter default value.
new FooPlan(foo.size())
```


if proto field `int64 size = 1;`, whose default value is 0, maps to a Spark `Long`
parameter whose default value is also 0, then we just use `int64 size = 1;` in the message.


If they are not the same, the proto field must be wrapped into a message. By doing so, we
could differentiate whether the field is set or not set. And when the field is not set,
Spark will use the defined default value.

For example:
```Scala
// Spark Plan.
case class FooPlan(size: Long = 100)
```

```protobuf
// Connect proto.
message Foo {
  message Size {
    int64 size;
  }
  
  Size size;
}
```
```Scala
// Spark access Connect proto.
foo = new proto.Foo()

if foo.hasSize() {
  // if foo is set, access the value from proto.
  new FooPlan(foo.size().size())
} else {
  // If size in Foo is not set, fall back to Spark parameter default value.
  new FooPlan()
}
```