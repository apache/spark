all: a b c d e f g h i j k l m n o p q r s t u v w x y" z

findBroken:
	sbt -Dshark.hive.alltests -Dshark.hive.failFast "test-only catalyst.execution.HiveCompatibility"

a:
	 sbt -Dshark.hive.whitelist=a.* "test-only catalyst.execution.HiveCompatibility"
b:
	 sbt -Dshark.hive.whitelist=b.* "test-only catalyst.execution.HiveCompatibility"
c:
	 sbt -Dshark.hive.whitelist=c.* "test-only catalyst.execution.HiveCompatibility"
d:
	 sbt -Dshark.hive.whitelist=d.* "test-only catalyst.execution.HiveCompatibility"
e:
	 sbt -Dshark.hive.whitelist=e.* "test-only catalyst.execution.HiveCompatibility"
f:
	 sbt -Dshark.hive.whitelist=f.* "test-only catalyst.execution.HiveCompatibility"
g:
	 sbt -Dshark.hive.whitelist=g.* "test-only catalyst.execution.HiveCompatibility"
h:
	 sbt -Dshark.hive.whitelist=h.* "test-only catalyst.execution.HiveCompatibility"
i:
	 sbt -Dshark.hive.whitelist=i.* "test-only catalyst.execution.HiveCompatibility"
j:
	 sbt -Dshark.hive.whitelist=j.* "test-only catalyst.execution.HiveCompatibility"
k:
	 sbt -Dshark.hive.whitelist=k.* "test-only catalyst.execution.HiveCompatibility"
l:
	 sbt -Dshark.hive.whitelist=l.* "test-only catalyst.execution.HiveCompatibility"
m:
	 sbt -Dshark.hive.whitelist=m.* "test-only catalyst.execution.HiveCompatibility"
n:
	 sbt -Dshark.hive.whitelist=n.* "test-only catalyst.execution.HiveCompatibility"
o:
	 sbt -Dshark.hive.whitelist=o.* "test-only catalyst.execution.HiveCompatibility"
p:
	 sbt -Dshark.hive.whitelist=p.* "test-only catalyst.execution.HiveCompatibility"
q:
	 sbt -Dshark.hive.whitelist=q.* "test-only catalyst.execution.HiveCompatibility"
r:
	 sbt -Dshark.hive.whitelist=r.* "test-only catalyst.execution.HiveCompatibility"
s:
	 sbt -Dshark.hive.whitelist=s.* "test-only catalyst.execution.HiveCompatibility"
t:
	 sbt -Dshark.hive.whitelist=t.* "test-only catalyst.execution.HiveCompatibility"
u:
	 sbt -Dshark.hive.whitelist=u.* "test-only catalyst.execution.HiveCompatibility"
v:
	 sbt -Dshark.hive.whitelist=v.* "test-only catalyst.execution.HiveCompatibility"
w:
	 sbt -Dshark.hive.whitelist=w.* "test-only catalyst.execution.HiveCompatibility"
x:
	 sbt -Dshark.hive.whitelist=x.* "test-only catalyst.execution.HiveCompatibility"
y:
	 sbt -Dshark.hive.whitelist=y.* "test-only catalyst.execution.HiveCompatibility"
z:
	 sbt -Dshark.hive.whitelist=z.* "test-only catalyst.execution.HiveCompatibility"
