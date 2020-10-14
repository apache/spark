select d_year, s_city, p_brand1, sum(lo_revenue-lo_supplycost) as profit1
	from date, customer, supplier, part, lineorder
	where lo_custkey = c_custkey
		and lo_suppkey = s_suppkey
		and lo_partkey = p_partkey
		and lo_orderdate = d_datekey
		and c_region = 'AMERICA'
		and s_nation = 'UNITED STATES'
		and (d_year = 1997 or d_year = 1998)
		and p_category = 'MFGR#14'
	group by d_year, s_city, p_brand1
	order by d_year, s_city, p_brand1
