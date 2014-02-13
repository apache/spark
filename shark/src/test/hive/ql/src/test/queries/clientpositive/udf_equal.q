DESCRIBE FUNCTION =;
DESCRIBE FUNCTION EXTENDED =;

DESCRIBE FUNCTION ==;
DESCRIBE FUNCTION EXTENDED ==;

SELECT true=false, false=true, false=false, true=true, NULL=NULL, true=NULL, NULL=true, false=NULL, NULL=false FROM src LIMIT 1;

DESCRIBE FUNCTION <=>;
DESCRIBE FUNCTION EXTENDED <=>;

SELECT true<=>false, false<=>true, false<=>false, true<=>true, NULL<=>NULL, true<=>NULL, NULL<=>true, false<=>NULL, NULL<=>false FROM src LIMIT 1;
