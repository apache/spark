-- was negative/ambiguous_table_col.q

drop table ambiguous;
create table ambiguous (key string, value string);

FROM src key
INSERT OVERWRITE TABLE ambiguous SELECT key.key, key.value WHERE key.value < 'val_100';

drop table ambiguous;
