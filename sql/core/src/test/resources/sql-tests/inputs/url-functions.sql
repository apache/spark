-- parse_url function
select parse_url('http://userinfo@spark.apache.org/path?query=1#Ref', 'HOST');
select parse_url('http://userinfo@spark.apache.org/path?query=1#Ref', 'PATH');
select parse_url('http://userinfo@spark.apache.org/path?query=1#Ref', 'QUERY');
select parse_url('http://userinfo@spark.apache.org/path?query=1#Ref', 'REF');
select parse_url('http://userinfo@spark.apache.org/path?query=1#Ref', 'PROTOCOL');
select parse_url('http://userinfo@spark.apache.org/path?query=1#Ref', 'FILE');
select parse_url('http://userinfo@spark.apache.org/path?query=1#Ref', 'AUTHORITY');
select parse_url('http://userinfo@spark.apache.org/path?query=1#Ref', 'USERINFO');

-- url_encode function
select url_encode('https://spark.apache.org');
select url_encode('inva lid://user:pass@host/file\\;param?query\\;p2');
select url_encode(null);

-- url_decode function
select url_decode('https%3A%2F%2Fspark.apache.org');
select url_decode('inva lid://user:pass@host/file\\;param?query\\;p2');
select url_decode(null);