with jsonStrings as (
  select
    id,
    format_string(
      '{"arr":[5,%s,15],"numeric":%s,"decimal":%f,"str":"%s","struct":{"child":%s},"ts":"%s","arrayOfStructs":[{"b":"%s"},{"b":null},{"%s":null},{"diffKey":%s}]}',
      id,
      id,
      cast(id as double) / 10,
      id,
      id,
      cast(
        (make_date(2024, 03, 01) - cast(id as int)) as string
      ),
      id,
      id,
      id
    ) as jsonString
  from
    range(0, 10000)
)
select
  id,
  parse_json(jsonString) as v,
  array(
    parse_json(jsonString),
    null,
    parse_json(jsonString),
    null,
    parse_json(jsonString)
  ) as array_of_variants,
  named_struct('v', parse_json(jsonString)) as struct_of_variants,
  map(
    cast(id as string),
    parse_json(jsonString),
    'nullKey',
    null
  ) as map_of_variants,
  array(
    named_struct('v', parse_json(jsonString)),
    named_struct('v', null),
    null,
    named_struct(
      'v',
      parse_json(jsonString)
    ),
    null,
    named_struct(
      'v',
      parse_json(jsonString)
    )
  ) as array_of_struct_of_variants,
  named_struct(
    'v',
    array(
      null,
      parse_json(jsonString)
    )
  ) as struct_of_array_of_variants,
  id % 3 as partitionKey
from
  jsonStrings
