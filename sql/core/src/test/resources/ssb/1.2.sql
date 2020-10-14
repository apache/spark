select sum(lo_extendedprice*lo_discount) as revenue
	from lineorder, date
	where lo_orderdate = d_datekey
		and d_yearmonthnum = 199401
		and lo_discount between 4 and 6
		and lo_quantity between 26 and 35
