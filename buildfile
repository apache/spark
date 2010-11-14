require 'buildr/scala'

# Version number for this release
VERSION_NUMBER = "0.0.0"
# Group identifier for your projects
GROUP = "spark"
COPYRIGHT = ""

# Specify Maven 2.0 remote repositories here, like this:
repositories.remote << "http://www.ibiblio.org/maven2/"

THIRD_PARTY_JARS = Dir["third_party/**/*.jar"]

desc "The Spark project"
define "spark" do
  project.version = VERSION_NUMBER
  project.group = GROUP
  manifest["Implementation-Vendor"] = COPYRIGHT
  compile.with THIRD_PARTY_JARS
  compile.using :scalac
  package(:jar)
  test.using :scalatest, :fork => true
end
