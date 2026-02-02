DESCRIBE FUNCTION parse_url;
DESCRIBE FUNCTION EXTENDED parse_url;

EXPLAIN
SELECT parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'HOST'), 
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'PATH'), 
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'QUERY') ,
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'REF') ,
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'QUERY', 'k2') ,
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'QUERY', 'k1') ,
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'QUERY', 'k3') ,
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'FILE') ,
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'PROTOCOL') ,
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'USERINFO') ,
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'AUTHORITY') 
  FROM src WHERE key = 86;

SELECT parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'HOST'), 
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'PATH'), 
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'QUERY') ,
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'REF') ,
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'QUERY', 'k2') ,
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'QUERY', 'k1') ,
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'QUERY', 'k3') ,
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'FILE') ,
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'PROTOCOL') ,
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'USERINFO') ,
parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'AUTHORITY') 
  FROM src WHERE key = 86;