<!-- NOT a stability contract: review-trigger snapshot only. Downstream consumers MUST NOT rely on byte equality across Spark versions. -->
## ICU sort-key bytes (review-trigger snapshot, NOT a stability contract)

| collation | input | sortkey (hex) |
|---|---|---|
| UNICODE | a | 2b 01 05 01 05 00 |
| UNICODE | b | 2d 01 05 01 05 00 |
| UNICODE | c | 2f 01 05 01 05 00 |
| UNICODE | A | 2b 01 05 01 dc 00 |
| UNICODE | B | 2d 01 05 01 dc 00 |
| UNICODE | C | 2f 01 05 01 dc 00 |
| UNICODE_CI | a | 2b 01 05 00 |
| UNICODE_CI | A | 2b 01 05 00 |
| UNICODE_CI | b | 2d 01 05 00 |
| UNICODE_CI | B | 2d 01 05 00 |
| en_USA | a | 2b 01 05 01 05 00 |
| en_USA | A | 2b 01 05 01 dc 00 |
| en_USA | b | 2d 01 05 01 05 00 |
| en_USA | B | 2d 01 05 01 dc 00 |
| UNICODE | e | 33 01 05 01 05 00 |
| UNICODE_AI | e | 33 01 00 |
| UNICODE_AI | é | 33 01 00 |
| en_USA_AI | e | 33 01 00 |
| en_USA_AI | é | 33 01 00 |
| UNICODE | é | 33 01 45 88 01 06 00 |
| UNICODE | é | 33 01 45 88 01 06 00 |
| UNICODE | ạ́ | 2b 01 45 88 c4 01 07 00 |
| UNICODE | ạ́ | 2b 01 45 c4 88 01 07 00 |
| UNICODE | 😀 | 0d 47 0e 01 05 01 05 00 |
| UNICODE | 𐀀 | 7f 80 09 01 05 01 05 00 |
| UNICODE | 한글 | 7c 2a 67 b9 06 8b c1 01 0a 01 0a 00 |
| UNICODE | a-b | 2b 05 0e 2d 01 07 01 07 00 |
| UNICODE | a b | 2b 04 2d 01 07 01 07 00 |
| en_USA | i | 3b 01 05 01 05 00 |
| en_USA | I | 3b 01 05 01 dc 00 |
| en_USA | ı | 3c 06 01 05 01 05 00 |
| en_USA | İ | 3b 01 45 9c 01 dc 05 00 |
| tr | i | 3b 01 05 01 05 00 |
| tr | I | 3a 1b 01 05 01 c6 00 |
| tr | ı | 3a 1b 01 05 01 05 00 |
| tr | İ | 3b 01 05 01 c6 00 |
| UNICODE | 北京 | fa 32 8c fa 0a 90 01 06 01 06 00 |
| UNICODE | 中国 | fa 04 a4 fa 5d b2 01 06 01 06 00 |
| UNICODE |  | 01 01 00 |
| UNICODE | � | ff fd 01 05 01 05 00 |
| UNICODE | \u0000 | 01 01 00 |
| UNICODE | \u001f | 01 01 00 |
| UNICODE | ︎ | 01 01 00 |
| UNICODE | ️ | 01 01 00 |
| sr | б | 28 0c 01 05 01 05 00 |
| sr | ђ | 28 20 01 05 01 05 00 |
| sr_Latn | č | 30 02 01 05 01 05 00 |
| sr_Latn | đ | 32 03 01 05 01 05 00 |
| zh | 匙 | 83 fd 01 05 01 05 00 |
| zh | 啦 | 6d fe 01 05 01 05 00 |
| zh | 哇 | 8a 0a 01 05 01 05 00 |
| zh_Hant | 丁 | 42 03 01 05 01 05 00 |
| ja | 々 | 0d 85 01 05 01 05 00 |
| ja | ー | 0d 95 01 05 01 05 00 |
| sv | å | 5e 79 02 01 05 01 05 00 |
| sv | ö | 5e 79 04 01 05 01 05 00 |
| de | ß | 4f 4f 01 70 05 01 c5 d3 00 |
| hi | ढ | 28 62 01 05 01 05 00 |
| ko | 郎 | a4 95 0e 01 05 01 05 00 |
