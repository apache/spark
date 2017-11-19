-- start query 3 in stream 0 using template query3.tpl
select
  dt.d_year,
  item.i_brand_id brand_id,
  item.i_brand brand,
  sum(ss_net_profit) sum_agg
from
  date_dim dt,
  store_sales,
  item
where
  dt.d_date_sk = store_sales.ss_sold_date_sk
  and store_sales.ss_item_sk = item.i_item_sk
  and item.i_manufact_id = 436
  and dt.d_moy = 12
  -- partition key filters
  and ( 
ss_sold_date_sk between 2415355 and 2415385
or ss_sold_date_sk between 2415720 and 2415750
or ss_sold_date_sk between 2416085 and 2416115
or ss_sold_date_sk between 2416450 and 2416480
or ss_sold_date_sk between 2416816 and 2416846
or ss_sold_date_sk between 2417181 and 2417211
or ss_sold_date_sk between 2417546 and 2417576
or ss_sold_date_sk between 2417911 and 2417941
or ss_sold_date_sk between 2418277 and 2418307
or ss_sold_date_sk between 2418642 and 2418672
or ss_sold_date_sk between 2419007 and 2419037
or ss_sold_date_sk between 2419372 and 2419402
or ss_sold_date_sk between 2419738 and 2419768
or ss_sold_date_sk between 2420103 and 2420133
or ss_sold_date_sk between 2420468 and 2420498
or ss_sold_date_sk between 2420833 and 2420863
or ss_sold_date_sk between 2421199 and 2421229
or ss_sold_date_sk between 2421564 and 2421594
or ss_sold_date_sk between 2421929 and 2421959
or ss_sold_date_sk between 2422294 and 2422324
or ss_sold_date_sk between 2422660 and 2422690
or ss_sold_date_sk between 2423025 and 2423055
or ss_sold_date_sk between 2423390 and 2423420
or ss_sold_date_sk between 2423755 and 2423785
or ss_sold_date_sk between 2424121 and 2424151
or ss_sold_date_sk between 2424486 and 2424516
or ss_sold_date_sk between 2424851 and 2424881
or ss_sold_date_sk between 2425216 and 2425246
or ss_sold_date_sk between 2425582 and 2425612
or ss_sold_date_sk between 2425947 and 2425977
or ss_sold_date_sk between 2426312 and 2426342
or ss_sold_date_sk between 2426677 and 2426707
or ss_sold_date_sk between 2427043 and 2427073
or ss_sold_date_sk between 2427408 and 2427438
or ss_sold_date_sk between 2427773 and 2427803
or ss_sold_date_sk between 2428138 and 2428168
or ss_sold_date_sk between 2428504 and 2428534
or ss_sold_date_sk between 2428869 and 2428899
or ss_sold_date_sk between 2429234 and 2429264
or ss_sold_date_sk between 2429599 and 2429629
or ss_sold_date_sk between 2429965 and 2429995
or ss_sold_date_sk between 2430330 and 2430360
or ss_sold_date_sk between 2430695 and 2430725
or ss_sold_date_sk between 2431060 and 2431090
or ss_sold_date_sk between 2431426 and 2431456
or ss_sold_date_sk between 2431791 and 2431821
or ss_sold_date_sk between 2432156 and 2432186
or ss_sold_date_sk between 2432521 and 2432551
or ss_sold_date_sk between 2432887 and 2432917
or ss_sold_date_sk between 2433252 and 2433282
or ss_sold_date_sk between 2433617 and 2433647
or ss_sold_date_sk between 2433982 and 2434012
or ss_sold_date_sk between 2434348 and 2434378
or ss_sold_date_sk between 2434713 and 2434743
or ss_sold_date_sk between 2435078 and 2435108
or ss_sold_date_sk between 2435443 and 2435473
or ss_sold_date_sk between 2435809 and 2435839
or ss_sold_date_sk between 2436174 and 2436204
or ss_sold_date_sk between 2436539 and 2436569
or ss_sold_date_sk between 2436904 and 2436934
or ss_sold_date_sk between 2437270 and 2437300
or ss_sold_date_sk between 2437635 and 2437665
or ss_sold_date_sk between 2438000 and 2438030
or ss_sold_date_sk between 2438365 and 2438395
or ss_sold_date_sk between 2438731 and 2438761
or ss_sold_date_sk between 2439096 and 2439126
or ss_sold_date_sk between 2439461 and 2439491
or ss_sold_date_sk between 2439826 and 2439856
or ss_sold_date_sk between 2440192 and 2440222
or ss_sold_date_sk between 2440557 and 2440587
or ss_sold_date_sk between 2440922 and 2440952
or ss_sold_date_sk between 2441287 and 2441317
or ss_sold_date_sk between 2441653 and 2441683
or ss_sold_date_sk between 2442018 and 2442048
or ss_sold_date_sk between 2442383 and 2442413
or ss_sold_date_sk between 2442748 and 2442778
or ss_sold_date_sk between 2443114 and 2443144
or ss_sold_date_sk between 2443479 and 2443509
or ss_sold_date_sk between 2443844 and 2443874
or ss_sold_date_sk between 2444209 and 2444239
or ss_sold_date_sk between 2444575 and 2444605
or ss_sold_date_sk between 2444940 and 2444970
or ss_sold_date_sk between 2445305 and 2445335
or ss_sold_date_sk between 2445670 and 2445700
or ss_sold_date_sk between 2446036 and 2446066
or ss_sold_date_sk between 2446401 and 2446431
or ss_sold_date_sk between 2446766 and 2446796
or ss_sold_date_sk between 2447131 and 2447161
or ss_sold_date_sk between 2447497 and 2447527
or ss_sold_date_sk between 2447862 and 2447892
or ss_sold_date_sk between 2448227 and 2448257
or ss_sold_date_sk between 2448592 and 2448622
or ss_sold_date_sk between 2448958 and 2448988
or ss_sold_date_sk between 2449323 and 2449353
or ss_sold_date_sk between 2449688 and 2449718
or ss_sold_date_sk between 2450053 and 2450083
or ss_sold_date_sk between 2450419 and 2450449
or ss_sold_date_sk between 2450784 and 2450814
or ss_sold_date_sk between 2451149 and 2451179
or ss_sold_date_sk between 2451514 and 2451544
or ss_sold_date_sk between 2451880 and 2451910
or ss_sold_date_sk between 2452245 and 2452275
or ss_sold_date_sk between 2452610 and 2452640
or ss_sold_date_sk between 2452975 and 2453005
or ss_sold_date_sk between 2453341 and 2453371
or ss_sold_date_sk between 2453706 and 2453736
or ss_sold_date_sk between 2454071 and 2454101
or ss_sold_date_sk between 2454436 and 2454466
or ss_sold_date_sk between 2454802 and 2454832
or ss_sold_date_sk between 2455167 and 2455197
or ss_sold_date_sk between 2455532 and 2455562
or ss_sold_date_sk between 2455897 and 2455927
or ss_sold_date_sk between 2456263 and 2456293
or ss_sold_date_sk between 2456628 and 2456658
or ss_sold_date_sk between 2456993 and 2457023
or ss_sold_date_sk between 2457358 and 2457388
or ss_sold_date_sk between 2457724 and 2457754
or ss_sold_date_sk between 2458089 and 2458119
or ss_sold_date_sk between 2458454 and 2458484
or ss_sold_date_sk between 2458819 and 2458849
or ss_sold_date_sk between 2459185 and 2459215
or ss_sold_date_sk between 2459550 and 2459580
or ss_sold_date_sk between 2459915 and 2459945
or ss_sold_date_sk between 2460280 and 2460310
or ss_sold_date_sk between 2460646 and 2460676
or ss_sold_date_sk between 2461011 and 2461041
or ss_sold_date_sk between 2461376 and 2461406
or ss_sold_date_sk between 2461741 and 2461771
or ss_sold_date_sk between 2462107 and 2462137
or ss_sold_date_sk between 2462472 and 2462502
or ss_sold_date_sk between 2462837 and 2462867
or ss_sold_date_sk between 2463202 and 2463232
or ss_sold_date_sk between 2463568 and 2463598
or ss_sold_date_sk between 2463933 and 2463963
or ss_sold_date_sk between 2464298 and 2464328
or ss_sold_date_sk between 2464663 and 2464693
or ss_sold_date_sk between 2465029 and 2465059
or ss_sold_date_sk between 2465394 and 2465424
or ss_sold_date_sk between 2465759 and 2465789
or ss_sold_date_sk between 2466124 and 2466154
or ss_sold_date_sk between 2466490 and 2466520
or ss_sold_date_sk between 2466855 and 2466885
or ss_sold_date_sk between 2467220 and 2467250
or ss_sold_date_sk between 2467585 and 2467615
or ss_sold_date_sk between 2467951 and 2467981
or ss_sold_date_sk between 2468316 and 2468346
or ss_sold_date_sk between 2468681 and 2468711
or ss_sold_date_sk between 2469046 and 2469076
or ss_sold_date_sk between 2469412 and 2469442
or ss_sold_date_sk between 2469777 and 2469807
or ss_sold_date_sk between 2470142 and 2470172
or ss_sold_date_sk between 2470507 and 2470537
or ss_sold_date_sk between 2470873 and 2470903
or ss_sold_date_sk between 2471238 and 2471268
or ss_sold_date_sk between 2471603 and 2471633
or ss_sold_date_sk between 2471968 and 2471998
or ss_sold_date_sk between 2472334 and 2472364
or ss_sold_date_sk between 2472699 and 2472729
or ss_sold_date_sk between 2473064 and 2473094
or ss_sold_date_sk between 2473429 and 2473459
or ss_sold_date_sk between 2473795 and 2473825
or ss_sold_date_sk between 2474160 and 2474190
or ss_sold_date_sk between 2474525 and 2474555
or ss_sold_date_sk between 2474890 and 2474920
or ss_sold_date_sk between 2475256 and 2475286
or ss_sold_date_sk between 2475621 and 2475651
or ss_sold_date_sk between 2475986 and 2476016
or ss_sold_date_sk between 2476351 and 2476381
or ss_sold_date_sk between 2476717 and 2476747
or ss_sold_date_sk between 2477082 and 2477112
or ss_sold_date_sk between 2477447 and 2477477
or ss_sold_date_sk between 2477812 and 2477842
or ss_sold_date_sk between 2478178 and 2478208
or ss_sold_date_sk between 2478543 and 2478573
or ss_sold_date_sk between 2478908 and 2478938
or ss_sold_date_sk between 2479273 and 2479303
or ss_sold_date_sk between 2479639 and 2479669
or ss_sold_date_sk between 2480004 and 2480034
or ss_sold_date_sk between 2480369 and 2480399
or ss_sold_date_sk between 2480734 and 2480764
or ss_sold_date_sk between 2481100 and 2481130
or ss_sold_date_sk between 2481465 and 2481495
or ss_sold_date_sk between 2481830 and 2481860
or ss_sold_date_sk between 2482195 and 2482225
or ss_sold_date_sk between 2482561 and 2482591
or ss_sold_date_sk between 2482926 and 2482956
or ss_sold_date_sk between 2483291 and 2483321
or ss_sold_date_sk between 2483656 and 2483686
or ss_sold_date_sk between 2484022 and 2484052
or ss_sold_date_sk between 2484387 and 2484417
or ss_sold_date_sk between 2484752 and 2484782
or ss_sold_date_sk between 2485117 and 2485147
or ss_sold_date_sk between 2485483 and 2485513
or ss_sold_date_sk between 2485848 and 2485878
or ss_sold_date_sk between 2486213 and 2486243
or ss_sold_date_sk between 2486578 and 2486608
or ss_sold_date_sk between 2486944 and 2486974
or ss_sold_date_sk between 2487309 and 2487339
or ss_sold_date_sk between 2487674 and 2487704
or ss_sold_date_sk between 2488039 and 2488069
)
group by
  dt.d_year,
  item.i_brand,
  item.i_brand_id
order by
  dt.d_year,
  sum_agg desc,
  brand_id
limit 100
-- end query 3 in stream 0 using template query3.tpl
