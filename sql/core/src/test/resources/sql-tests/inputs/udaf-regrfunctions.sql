--
--   Licensed to the Apache Software Foundation (ASF) under one or more
--   contributor license agreements.  See the NOTICE file distributed with
--   this work for additional information regarding copyright ownership.
--   The ASF licenses this file to You under the Apache License, Version 2.0
--   (the "License"); you may not use this file except in compliance with
--   the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.
--

CREATE OR REPLACE TEMPORARY VIEW t1 AS SELECT * FROM VALUES
 (101, 1, 1, 1),
 (201, 2, 1, 1),
 (301, 3, 1, 1),
 (401, 4, 1, 11),
 (501, 5, 1, null),
 (601, 6, null, 1),
 (701, 6, null, null),
 (102, 1, 2, 2),
 (202, 2, 1, 2),
 (302, 3, 2, 1),
 (402, 4, 2, 12),
 (502, 5, 2, null),
 (602, 6, null, 2),
 (702, 6, null, null),
 (103, 1, 3, 3),
 (203, 2, 1, 3),
 (303, 3, 3, 1),
 (403, 4, 3, 13),
 (503, 5, 3, null),
 (603, 6, null, 3),
 (703, 6, null, null),
 (104, 1, 4, 4),
 (204, 2, 1, 4),
 (304, 3, 4, 1),
 (404, 4, 4, 14),
 (504, 5, 4, null),
 (604, 6, null, 4),
 (704, 6, null, null),
 (800, 7, 1, 1)
as t1(id, px, y, x);

select px, var_pop(x), var_pop(y), corr(y,x), covar_samp(y,x), covar_pop(y,x), regr_count(y,x),
 regr_slope(y,x), regr_intercept(y,x), regr_r2(y,x), regr_sxx(y,x), regr_syy(y,x), regr_sxy(y,x),
 regr_avgx(y,x), regr_avgy(y,x), regr_count(y,x)
from t1 group by px order by px;


select id, regr_count(y,x) over (partition by px) from t1 order by id;
