-- Create temp view with Variant columns for testing field extraction and type casting.
CREATE TEMP VIEW variant_test_data AS
SELECT
  parse_json('{ "price": 30 }') as int_price_variant,
  parse_json('{ "price": 12345.678 }') as double_price_variant,
  parse_json('{ "name": "John", "age": 30, "city": "New York", "active": true, "scores": [85, 92, 78] }') as multi_field_variant,
  parse_json('{ "item": [ { "model" : "basic", "price" : 6.12 }, { "model" : "medium", "price" : 9.24 } ] }') as array_value_variant,
  parse_json('[{ "id": 1, "name": "Alice" }, { "id": 2, "name": "Bob" }, { "id": 3, "name": "Charlie" }]') as array_variant,
  parse_json('{ "metadata": { "version": "1.0", "tags": ["important", "urgent"], "nested": { "level": 2, "value": "deep" } } }') as nested_variant,
  parse_json('{ "field-name": "value1", "field.name": "value2", "field_name": "value3" }') as special_chars_variant;

-- Single field extraction and type casting.
select int_price_variant:price from variant_test_data;
select int_price_variant:price::decimal(5, 2) from variant_test_data;
select int_price_variant:price::string from variant_test_data;

-- Applying an invalid function -- will throw an error.
select double_price_variant:price::decimal(3, 2) from variant_test_data;

-- Multi-field access.
select multi_field_variant:name, multi_field_variant:age, multi_field_variant:city from variant_test_data;
select multi_field_variant:name::string, multi_field_variant:age::int, multi_field_variant:active::boolean from variant_test_data;
select multi_field_variant:['name'] from variant_test_data;
select multi_field_variant:['age'] from variant_test_data;

-- Array value access.
select array_value_variant:item[0].model from variant_test_data;
select array_value_variant:item[0].price from variant_test_data;
select array_value_variant:item[1].model from variant_test_data;
select array_value_variant:item[1].price from variant_test_data;

-- Array access.
select array_variant:[0].id from variant_test_data;
select array_variant:[0].name from variant_test_data;
select array_variant:[1].id from variant_test_data;
select array_variant:[1].name from variant_test_data;

-- Nested field access.
select nested_variant:metadata.version from variant_test_data;
select nested_variant:metadata.tags[0] from variant_test_data;
select nested_variant:metadata.nested.level from variant_test_data;
select nested_variant:metadata.nested.value from variant_test_data;
select nested_variant:metadata['version'] from variant_test_data;
select nested_variant:metadata['tags'] from variant_test_data;

-- Special characters.
select special_chars_variant:`field-name`::string from variant_test_data;
select special_chars_variant:['field-name']::string from variant_test_data;
select special_chars_variant:field_name::string from variant_test_data;
select special_chars_variant:['field_name']::string from variant_test_data;
-- Not supported; will return NULL.
select special_chars_variant:`field.name`::string from variant_test_data;
-- Using [] is okay.
select special_chars_variant:['field.name']::string from variant_test_data;

-- Array operations on Variant arrays.
select multi_field_variant:scores[0]::int + multi_field_variant:scores[1]::int from variant_test_data;
select count(*) from (select explode(cast(multi_field_variant:scores as array<int>)) as score from variant_test_data);

-- ASTERISK syntax.
select * from variant_test_data;
-- Not supported; will throw an error.
select multi_field_variant:* from variant_test_data;

-- Type checking: The result of the following would all be 'variant'.
select typeof(multi_field_variant:name) from variant_test_data;
select typeof(multi_field_variant:age) from variant_test_data;
select typeof(multi_field_variant:active) from variant_test_data;
select typeof(multi_field_variant:scores) from variant_test_data;

-- Variant field access with NULL handling.
select isnull(multi_field_variant:missing_field) from variant_test_data;
select isnotnull(multi_field_variant:name) from variant_test_data;
select coalesce(multi_field_variant:missing_field, 'default_value') from variant_test_data;
