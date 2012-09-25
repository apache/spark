require 'fileutils'
include FileUtils

if ENV['SKIP_SCALADOC'] != '1'
  projects = ["core", "examples", "repl", "bagel"]

  puts "Moving to project root and building scaladoc."
  curr_dir = pwd
  cd("..")

  puts "Running sbt/sbt doc from " + pwd + "; this may take a few minutes..."
  puts `sbt/sbt doc`

  puts "moving back into docs dir."
  cd("docs")

  # Copy over the scaladoc from each project into the docs directory.
  # This directory will be copied over to _site when `jekyll` command is run.
  projects.each do |project_name|
    source = "../" + project_name + "/target/scala-2.9.2/api"
    dest = "api/" + project_name

    puts "echo making directory " + dest
    mkdir_p dest

    # From the rubydoc: cp_r('src', 'dest') makes src/dest, but this doesn't.
    puts "cp -r " + source + "/. " + dest
    cp_r(source + "/.", dest)
  end
end
