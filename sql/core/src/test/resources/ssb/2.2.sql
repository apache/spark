select sum(lo_revenue), d_year, p_brand1
	from lineorder, date, part, supplier
	where lo_orderdate = d_datekey
		and lo_partkey = p_partkey
		and lo_suppkey = s_suppkey
		and p_brand1 between 'MFGR#2221' and 'MFGR#2228'
		and s_region = 'ASIA'
	group by d_year, p_brand1
	order by d_year, p_brand1
