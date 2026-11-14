<!-- Review trigger for ICU upgrades. Section 1 bytes feed hash(), xxhash64() and sketch aggregates on collated strings; section 2 is the ORDER BY order. A change here changes user-visible values. Not a promise that they stay fixed. -->

ICU 78.3

## 1. Sort keys

| collation | input | code points | sort key (hex) |
|---|---|---|---|
| UNICODE | a | U+0061 | 2b 01 05 01 05 00 |
| UNICODE | b | U+0062 | 2d 01 05 01 05 00 |
| UNICODE | c | U+0063 | 2f 01 05 01 05 00 |
| UNICODE | A | U+0041 | 2b 01 05 01 dc 00 |
| UNICODE | B | U+0042 | 2d 01 05 01 dc 00 |
| UNICODE | C | U+0043 | 2f 01 05 01 dc 00 |
| UNICODE_CI | a | U+0061 | 2b 01 05 00 |
| UNICODE_CI | A | U+0041 | 2b 01 05 00 |
| UNICODE_CI | b | U+0062 | 2d 01 05 00 |
| UNICODE_CI | B | U+0042 | 2d 01 05 00 |
| en_USA | a | U+0061 | 2b 01 05 01 05 00 |
| en_USA | A | U+0041 | 2b 01 05 01 dc 00 |
| en_USA | b | U+0062 | 2d 01 05 01 05 00 |
| en_USA | B | U+0042 | 2d 01 05 01 dc 00 |
| UNICODE | e | U+0065 | 33 01 05 01 05 00 |
| UNICODE | é | U+00E9 | 33 01 45 88 01 06 00 |
| UNICODE | [U+0065 U+0301] | U+0065 U+0301 | 33 01 45 88 01 06 00 |
| UNICODE_AI | e | U+0065 | 33 01 00 |
| UNICODE_AI | é | U+00E9 | 33 01 00 |
| en_USA_AI | e | U+0065 | 33 01 00 |
| en_USA_AI | é | U+00E9 | 33 01 00 |
| UNICODE | [U+0061 U+0301 U+0323] | U+0061 U+0301 U+0323 | 2b 01 45 88 c4 01 07 00 |
| UNICODE | [U+0061 U+0323 U+0301] | U+0061 U+0323 U+0301 | 2b 01 45 c4 88 01 07 00 |
| UNICODE | 😀 | U+1F600 | 0d 47 0e 01 05 01 05 00 |
| UNICODE | 𐀀 | U+10000 | 7f 80 09 01 05 01 05 00 |
| UNICODE | 한글 | U+D55C U+AE00 | 7c 2a 67 b9 06 8b c1 01 0a 01 0a 00 |
| UNICODE | ab | U+0061 U+0062 | 2b 2d 01 06 01 06 00 |
| UNICODE | a-b | U+0061 U+002D U+0062 | 2b 05 0e 2d 01 07 01 07 00 |
| UNICODE | [U+0061 U+0020 U+0062] | U+0061 U+0020 U+0062 | 2b 04 2d 01 07 01 07 00 |
| en_USA | i | U+0069 | 3b 01 05 01 05 00 |
| en_USA | I | U+0049 | 3b 01 05 01 dc 00 |
| en_USA | ı | U+0131 | 3c 06 01 05 01 05 00 |
| en_USA | İ | U+0130 | 3b 01 45 9c 01 dc 05 00 |
| tr | i | U+0069 | 3b 01 05 01 05 00 |
| tr | I | U+0049 | 3a 1b 01 05 01 c6 00 |
| tr | ı | U+0131 | 3a 1b 01 05 01 05 00 |
| tr | İ | U+0130 | 3b 01 05 01 c6 00 |
| UNICODE | 北京 | U+5317 U+4EAC | fa 32 8c fa 0a 90 01 06 01 06 00 |
| UNICODE | 中国 | U+4E2D U+56FD | fa 04 a4 fa 5d b2 01 06 01 06 00 |
| UNICODE | [(empty)] | (empty) | 01 01 00 |
| UNICODE | � | U+FFFD | ff fd 01 05 01 05 00 |
| UNICODE | [U+0061 U+0000 U+0062] | U+0061 U+0000 U+0062 | 2b 2d 01 06 01 06 00 |
| UNICODE | [U+0061 U+001F U+0062] | U+0061 U+001F U+0062 | 2b 2d 01 06 01 06 00 |
| UNICODE | [U+0061 U+FE0E U+0062] | U+0061 U+FE0E U+0062 | 2b 2d 01 06 01 06 00 |
| UNICODE | [U+0061 U+FE0F U+0062] | U+0061 U+FE0F U+0062 | 2b 2d 01 06 01 06 00 |
| sr | б | U+0431 | 28 0c 01 05 01 05 00 |
| sr | ђ | U+0452 | 28 20 01 05 01 05 00 |
| sr_Latn | č | U+010D | 30 02 01 05 01 05 00 |
| sr_Latn | đ | U+0111 | 32 03 01 05 01 05 00 |
| zh | 匙 | U+5319 | 83 fd 01 05 01 05 00 |
| zh | 啦 | U+5566 | 6d fe 01 05 01 05 00 |
| zh | 哇 | U+54C7 | 8a 0a 01 05 01 05 00 |
| zh | 持 | U+6301 | 53 cd 01 05 01 05 00 |
| zh | 拉 | U+62C9 | 6d db 01 05 01 05 00 |
| zh | 誓 | U+8A93 | 83 e9 01 05 01 05 00 |
| zh | 收 | U+6536 | 84 03 01 05 01 05 00 |
| zh | 挖 | U+6316 | 89 eb 01 05 01 05 00 |
| zh_Hant | 丁 | U+4E01 | 42 03 01 05 01 05 00 |
| ja | 々 | U+3005 | 0d 85 01 05 01 05 00 |
| ja | ー | U+30FC | 0d 95 01 05 01 05 00 |
| sv | å | U+00E5 | 5e 79 02 01 05 01 05 00 |
| sv | ö | U+00F6 | 5e 79 04 01 05 01 05 00 |
| de | ß | U+00DF | 4f 4f 01 70 05 01 c5 d3 00 |
| hi | ढ | U+0922 | 28 62 01 05 01 05 00 |
| ko | 郎 | U+90CE | a4 95 0e 01 05 01 05 00 |

## 2. Comparator order (ORDER BY)

| collation | order |
|---|---|
| UNICODE | [(empty)] < 😀 < a < A < [U+0061 U+0301 U+0323] < [U+0061 U+0323 U+0301] < [U+0061 U+0020 U+0062] < a-b < ab = [U+0061 U+0000 U+0062] = [U+0061 U+001F U+0062] = [U+0061 U+FE0E U+0062] = [U+0061 U+FE0F U+0062] < b < B < c < C < e < é = [U+0065 U+0301] < 한글 < 𐀀 < 中国 < 北京 < � |
| UNICODE_CI | a = A < b = B |
| en_USA | a < A < b < B < i < I < İ < ı |
| UNICODE_AI | e = é |
| en_USA_AI | e = é |
| tr | ı < I < i < İ |
| sr | б < ђ |
| sr_Latn | č < đ |
| zh | 持 < 拉 < 啦 < 誓 < 匙 < 收 < 挖 < 哇 |
| zh_Hant | 丁 |
| ja | 々 < ー |
| sv | å < ö |
| de | ß |
| hi | ढ |
| ko | 郎 |
