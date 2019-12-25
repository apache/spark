SELECT 'abc' SIMILAR TO 'abc' AS `true`;

SELECT 'abc' SIMILAR TO 'a' AS `false`;

SELECT 'abc' SIMILAR TO '%(b|d)%' AS `true`;

SELECT 'abc' SIMILAR TO '(b|c)%' AS `false`;

SELECT '|bd' SIMILAR TO '|(b|d)*' AS `false`;

SELECT '|bd' SIMILAR TO '\\|(b|d)*' AS `true`;

SELECT '|bd' SIMILAR TO '\\|(b|d)*' ESCAPE '\\' AS `true`;

SELECT '|bd' SIMILAR TO '\\|(b|d)*' ESCAPE '#' AS `false`;

SELECT '|bd' SIMILAR TO '#|(b|d)*' ESCAPE '#' AS `true`;

SELECT 'abd' SIMILAR TO 'a(b|d)*' AS `true`;

SELECT 'abd' SIMILAR TO 'a.*' AS `false`;

SELECT 'abd' SIMILAR TO 'a%' AS `true`;

SELECT 'abc' SIMILAR TO '_b_' AS `true`;

SELECT 'abc' SIMILAR TO '_A_' AS `false`;

SELECT 'AbcAbcdefgefg12efgefg12' SIMILAR TO '((Ab)?c)+d((efg)+(12))+' AS `true`;

SELECT 'aaaaaab11111xy' SIMILAR TO 'a{6}_[0-9]{5}(x|y){2}' AS `true`;

SELECT '$0.87' SIMILAR TO '$[0-9]+(.[0-9][0-9])?' AS `true`;

SELECT '^0.87' SIMILAR TO '^[0-9]+(.[0-9][0-9])?' AS `true`;

SELECT '0.87$' SIMILAR TO '[0-9]+(.[0-9][0-9])?$' AS `true`;

SELECT '^0.87$' SIMILAR TO '^[0-9]+(.[0-9][0-9])?$' AS `true`;

SELECT 'ab_7' SIMILAR TO '(\\w)+' AS `true`;

SELECT 'ab_7' SIMILAR TO '(\\w)+' ESCAPE '\\' AS `true`;

SELECT 'ab_7' SIMILAR TO '(\\w)+' ESCAPE '#' AS `false`;

SELECT 'ab_7' SIMILAR TO '(#w)+' ESCAPE '#' AS `true`;

SELECT '%&#' SIMILAR TO '(\\w)+' AS `false`;

SELECT '%&#' SIMILAR TO '(\\W)+' AS `true`;

SELECT 'ab_7' SIMILAR TO '(\\W)+' AS `false`;

SELECT ' ' SIMILAR TO '(\\s)+' AS `true`;

SELECT '  ' SIMILAR TO '(\\s)+' AS `true`;

SELECT ' ' SIMILAR TO '(\\S)+' AS `false`;

SELECT '  ' SIMILAR TO '(\\S)+' AS `false`;

SELECT 'Ab$*' SIMILAR TO '(\\S)+' AS `true`;

SELECT 'Ab$*' SIMILAR TO '(\\s)+' AS `false`;

SELECT '5220' SIMILAR TO '(\\d)+' AS `true`;

SELECT '5i20' SIMILAR TO '(\\d)+' AS `false`;

SELECT '5220' SIMILAR TO '(\\D)+' AS `false`;

SELECT '@i(L' SIMILAR TO '(\\D)+' AS `true`;

SELECT '@i(L' SIMILAR TO null;

SELECT 'ab_7' SIMILAR TO null ESCAPE '\\';
