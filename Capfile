require 'bundler/setup'
require 'capistrano_recipes/deploy/packserv'

set :application, "spark"
set :user, "deploy"
set :shared_work_path, "/u/apps/spark/shared/work"
set :shared_logs_path, "/u/apps/spark/shared/log"
set :shared_conf_path, "/u/apps/spark/shared/conf"
set :spark_jar_path, "hdfs://hadoop-production/user/sparkles"
set :gateway, nil
set :keep_releases, 5
set :branch, fetch(:branch, `git symbolic-ref --short HEAD`.gsub("\s",""))
# turns out that fetch(:sha), when combined with packserv, will show only the latest sha on packserv
set :local_sha, `git rev-parse HEAD`.rstrip

DATANODES = (2..47).map {|i| "dn%02d.chi.shopify.com" % i }
OTHERNODES = ["hadoop-etl1.chi.shopify.com", "spark-etl1.chi.shopify.com", "reports-reportify-etl3.chi.shopify.com", "reports-reportify-skydb4.chi.shopify.com", "platfora2.chi.shopify.com"]
BROKEN = ["dn09.chi.shopify.com", "dn16.chi.shopify.com"] # Node is down don't try to send code

task :production do
  role :app, *(DATANODES + OTHERNODES - BROKEN)
  role :history, "hadoop-rm.chi.shopify.com"
  role :uploader, "spark-etl1.chi.shopify.com"
end

namespace :deploy do
  task :cleanup do
    count = fetch(:keep_releases, 5).to_i
    run "ls -1dt /u/apps/spark/releases/* | tail -n +#{count + 1} | xargs rm -rf"
  end

  #this needs to be re-enabled at some point. the problem with this is that if you test multiple SHAs of spark, you are potentially deleting production jars :/
  task :clear_hdfs_executables, :roles => :uploader, :on_no_matching_servers => :continue do
    count = fetch(:keep_releases, 5).to_i
    existing_releases = capture "hdfs dfs -ls #{fetch(:spark_jar_path)}/spark-assembly-*.jar | sort -k6 | sed 's/\\s\\+/ /g' | cut -d' ' -f8"
    existing_releases = existing_releases.split
    # hashtag paranoid. let the uploader overwrite the latest.jar
    existing_releases.reject! {|element| element.end_with?("spark-assembly-latest.jar")}
    if existing_releases.count > count
      existing_releases.shift(existing_releases.count - count).each do |path|
        run "hdfs dfs -rm #{path}"
      end
    end
  end

  task :upload_to_hdfs, :roles => :uploader, :on_no_matching_servers => :continue do
    raw_binary_path = "./assembly/target/scala-2.10/spark-assembly-1.3.0-SNAPSHOT-hadoop2.5.0.jar"
    modified_binary_path = "./lib/spark-assembly-#{fetch(:local_sha)}.jar"
    if fetch(:branch) == "master"
      run "hdfs dfs -copyFromLocal -f #{release_path}/lib/spark-assembly-*.jar hdfs://hadoop-production/user/sparkles/spark-assembly-#{fetch(:sha)}.jar"
    else
      unless File.exist?(modified_binary_path)
        system("mvn package -DskipTests -Phadoop-2.4 -Dhadoop.version=2.5.0 -Pyarn -Phive")
        system("mv #{raw_binary_path} #{modified_binary_path}")
      end
      system("hdfs dfs -copyFromLocal #{modified_binary_path} hdfs://nn01.chi.shopify.com/user/sparkles")
    end
  end

  task :test_spark_jar, :roles => :uploader, :on_no_master_servers => :continue do
    spark_yarn_jar_sha = fetch(:branch) == "master" ? fetch(:sha) : fetch(:local_sha)
    run "sudo -u azkaban sh -c '. /u/virtualenvs/starscream/bin/activate && cd /u/apps/starscream/current && PYTHON_ENV=production SPARK_OPTS=\"spark.yarn.jar=hdfs://hadoop-production/user/sparkles/spark-assembly-#{spark_yarn_jar_sha}.jar\" exec python shopify/tools/canary.py'"
  end

  task :prevent_gateway do
    set :gateway, nil
  end

  task :symlink_shared do
    run "ln -nfs #{shared_work_path} #{release_path}/work"
    run "ln -nfs #{shared_logs_path} #{release_path}/logs"
    run "rm -rf #{release_path}/conf && ln -nfs #{shared_conf_path} #{release_path}/conf"
  end

  task :remind_us_to_update_starscream do
    puts "****************************************************************"
    puts "*"
    puts "*    Remember to update starscream/conf/config.yml"
    puts "*"
    puts "*    spark_production"
    puts "*      conf_options:"
    puts "*      <<: *spark_remote"
    puts "*      spark.yarn.jar: \"#{fetch(:spark_jar_path)}/spark-assembly-\033[31m#{fetch(:sha)}\033[0m.jar\""
    puts "*"
    puts "****************************************************************"
  end


  task :restart do
  end

  after 'deploy:initialize_variables', 'deploy:prevent_gateway' # capistrano recipes packserv deploy always uses a gateway
  before  'deploy:symlink_current', 'deploy:symlink_shared'
  before  'deploy:test_spark_jar', 'deploy:initialize_variables'
  before  'deploy:upload_to_hdfs', 'deploy:initialize_variables'
  after  'deploy:download', 'deploy:upload_to_hdfs', 'deploy:test_spark_jar'
  after 'deploy:restart', 'deploy:cleanup'
  after 'deploy:cleanup', 'deploy:remind_us_to_update_starscream'
end
