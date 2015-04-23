SET hive.vectorized.execution.enabled = true;

-- Test type casting in vectorized mode to verify end-to-end functionality.

explain 
select 
-- to boolean
   cast (ctinyint as boolean)
  ,cast (csmallint as boolean)
  ,cast (cint as boolean)
  ,cast (cbigint as boolean)
  ,cast (cfloat as boolean)
  ,cast (cdouble as boolean)
  ,cast (cboolean1 as boolean)
  ,cast (cbigint * 0 as boolean)
  ,cast (ctimestamp1 as boolean)
  ,cast (cstring1 as boolean)
-- to int family
  ,cast (ctinyint as int)
  ,cast (csmallint as int)
  ,cast (cint as int)
  ,cast (cbigint as int)
  ,cast (cfloat as int)
  ,cast (cdouble as int)
  ,cast (cboolean1 as int)
  ,cast (ctimestamp1 as int)
  ,cast (cstring1 as int)
  ,cast (substr(cstring1, 1, 1) as int)
  ,cast (cfloat as tinyint)
  ,cast (cfloat as smallint)
  ,cast (cfloat as bigint)
-- to float family
  ,cast (ctinyint as double)
  ,cast (csmallint as double)
  ,cast (cint as double)
  ,cast (cbigint as double)
  ,cast (cfloat as double)
  ,cast (cdouble as double)
  ,cast (cboolean1 as double)
  ,cast (ctimestamp1 as double)
  ,cast (cstring1 as double)
  ,cast (substr(cstring1, 1, 1) as double)
  ,cast (cint as float)
  ,cast (cdouble as float)
-- to timestamp
  ,cast (ctinyint as timestamp)
  ,cast (csmallint as timestamp)
  ,cast (cint as timestamp)
  ,cast (cbigint as timestamp)
  ,cast (cfloat as timestamp)
  ,cast (cdouble as timestamp)
  ,cast (cboolean1 as timestamp)
  ,cast (cbigint * 0 as timestamp)
  ,cast (ctimestamp1 as timestamp)
  ,cast (cstring1 as timestamp)
  ,cast (substr(cstring1, 1, 1) as timestamp)
-- to string
  ,cast (ctinyint as string)
  ,cast (csmallint as string)
  ,cast (cint as string)
  ,cast (cbigint as string)
  ,cast (cfloat as string)
  ,cast (cdouble as string)
  ,cast (cboolean1 as string)
  ,cast (cbigint * 0 as string)
  ,cast (ctimestamp1 as string)
  ,cast (cstring1 as string)
-- nested and expression arguments
  ,cast (cast (cfloat as int) as float)
  ,cast (cint * 2 as double)
  ,cast (sin(cfloat) as string)
  ,cast (cint as float) + cast(cboolean1 as double)
from alltypesorc
-- limit output to a reasonably small number of rows
where cbigint % 250 = 0;


select 
-- to boolean
   cast (ctinyint as boolean)
  ,cast (csmallint as boolean)
  ,cast (cint as boolean)
  ,cast (cbigint as boolean)
  ,cast (cfloat as boolean)
  ,cast (cdouble as boolean)
  ,cast (cboolean1 as boolean)
  ,cast (cbigint * 0 as boolean)
  ,cast (ctimestamp1 as boolean)
  ,cast (cstring1 as boolean)
-- to int family
  ,cast (ctinyint as int)
  ,cast (csmallint as int)
  ,cast (cint as int)
  ,cast (cbigint as int)
  ,cast (cfloat as int)
  ,cast (cdouble as int)
  ,cast (cboolean1 as int)
  ,cast (ctimestamp1 as int)
  ,cast (cstring1 as int)
  ,cast (substr(cstring1, 1, 1) as int)
  ,cast (cfloat as tinyint)
  ,cast (cfloat as smallint)
  ,cast (cfloat as bigint)
-- to float family
  ,cast (ctinyint as double)
  ,cast (csmallint as double)
  ,cast (cint as double)
  ,cast (cbigint as double)
  ,cast (cfloat as double)
  ,cast (cdouble as double)
  ,cast (cboolean1 as double)
  ,cast (ctimestamp1 as double)
  ,cast (cstring1 as double)
  ,cast (substr(cstring1, 1, 1) as double)
  ,cast (cint as float)
  ,cast (cdouble as float)
-- to timestamp
  ,cast (ctinyint as timestamp)
  ,cast (csmallint as timestamp)
  ,cast (cint as timestamp)
  ,cast (cbigint as timestamp)
  ,cast (cfloat as timestamp)
  ,cast (cdouble as timestamp)
  ,cast (cboolean1 as timestamp)
  ,cast (cbigint * 0 as timestamp)
  ,cast (ctimestamp1 as timestamp)
  ,cast (cstring1 as timestamp)
  ,cast (substr(cstring1, 1, 1) as timestamp)
-- to string
  ,cast (ctinyint as string)
  ,cast (csmallint as string)
  ,cast (cint as string)
  ,cast (cbigint as string)
  ,cast (cfloat as string)
  ,cast (cdouble as string)
  ,cast (cboolean1 as string)
  ,cast (cbigint * 0 as string)
  ,cast (ctimestamp1 as string)
  ,cast (cstring1 as string)
-- nested and expression arguments
  ,cast (cast (cfloat as int) as float)
  ,cast (cint * 2 as double)
  ,cast (sin(cfloat) as string)
  ,cast (cint as float) + cast(cboolean1 as double)
from alltypesorc
-- limit output to a reasonably small number of rows
where cbigint % 250 = 0;

 