CREATE TABLE triples (foo string, subject string, predicate string, object string, foo2 string);

EXPLAIN
SELECT t11.subject, t22.object , t33.subject , t55.object, t66.object
FROM
(
SELECT t1.subject
FROM triples t1
WHERE
t1.predicate='http://sofa.semanticweb.org/sofa/v1.0/system#__INSTANCEOF_REL'
AND
t1.object='http://ontos/OntosMiner/Common.English/ontology#Citation'
) t11
JOIN
(
SELECT t2.subject , t2.object
FROM triples t2
WHERE
t2.predicate='http://sofa.semanticweb.org/sofa/v1.0/system#__LABEL_REL'
) t22
ON (t11.subject=t22.subject)
JOIN
(
SELECT t3.subject , t3.object
FROM triples t3
WHERE
t3.predicate='http://www.ontosearch.com/2007/12/ontosofa-ns#_from'

) t33
ON (t11.subject=t33.object)
JOIN
(
SELECT t4.subject
FROM triples t4
WHERE
t4.predicate='http://sofa.semanticweb.org/sofa/v1.0/system#__INSTANCEOF_REL'
AND
t4.object='http://ontos/OntosMiner/Common.English/ontology#Author'

) t44
ON (t44.subject=t33.subject)
JOIN
(
SELECT t5.subject, t5.object
FROM triples t5
WHERE
t5.predicate='http://www.ontosearch.com/2007/12/ontosofa-ns#_to'
) t55
ON (t55.subject=t44.subject)
JOIN
(
SELECT t6.subject, t6.object
FROM triples t6
WHERE
t6.predicate='http://sofa.semanticweb.org/sofa/v1.0/system#__LABEL_REL'
) t66
ON (t66.subject=t55.object);

