addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.11.2")

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "4.0.0")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.1.9")

addSbtPlugin("com.alpinenow" % "junit_xml_listener" % "0.5.1")

addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.3.3")

addSbtPlugin("com.cavorite" % "sbt-avro" % "0.3.2")

addSbtPlugin("io.spray" % "sbt-revolver" % "0.8.0")

libraryDependencies += "org.ow2.asm"  % "asm" % "5.0.3"

libraryDependencies += "org.ow2.asm"  % "asm-commons" % "5.0.3"

addSbtPlugin("com.simplytyped" % "sbt-antlr4" % "0.7.11")
