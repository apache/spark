-- Describe a list structure in a thrift table
describe src_thrift.lint;

-- Describe the element of a list
describe src_thrift.lint.$elem$;

-- Describe the key of a map
describe src_thrift.mStringString.$key$;

-- Describe the value of a map
describe src_thrift.mStringString.$value$;

-- Describe a complex element of a list
describe src_thrift.lintString.$elem$;

-- Describe a member of an element of a list
describe src_thrift.lintString.$elem$.myint;
