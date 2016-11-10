resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.11.2")

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.2.0")

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")

// For Sonatype publishing
//resolvers += Resolver.url("sbt-plugin-releases", new URL("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases/"))(Resolver.ivyStylePatterns)

//addSbtPlugin("com.jsuereth" % "xsbt-gpg-plugin" % "0.6")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.4")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.7.0")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.1.6")

addSbtPlugin("com.alpinenow" % "junit_xml_listener" % "0.5.1")

addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.3.3")

addSbtPlugin("com.cavorite" % "sbt-avro" % "0.3.2")

addSbtPlugin("io.spray" % "sbt-revolver" % "0.7.2")

libraryDependencies += "org.ow2.asm"  % "asm" % "5.0.3"

libraryDependencies += "org.ow2.asm"  % "asm-commons" % "5.0.3"

// Spark uses a custom fork of the sbt-pom-reader plugin which contains a patch to fix issues
// related to test-jar dependencies (https://github.com/sbt/sbt-pom-reader/pull/14). The source for
// this fork is published at https://github.com/JoshRosen/sbt-pom-reader/tree/v1.0.0-spark
// and corresponds to commit b160317fcb0b9d1009635a7c5aa05d0f3be61936 in that repository.
// In the long run, we should try to merge our patch upstream and switch to an upstream version of
// the plugin; this is tracked at SPARK-14401.

addSbtPlugin("org.spark-project" % "sbt-pom-reader" % "1.0.0-spark")
