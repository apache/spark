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

DATANODES = (2..73).map {|i| "dn%02d.chi.shopify.com" % i }
OTHERNODES = ["hadoop-misc4.chi.shopify.com", "dn48.chi.shopify.com", "reportify-etl4.chi.shopify.com", "streams3.chi.shopify.com", "streams4.chi.shopify.com"]
BROKEN = ["dn58.chi.shopify.com"] # Node is down don't try to send code

task :production do
  role :app, *(DATANODES + OTHERNODES - BROKEN)
  role :history, "hadoop-rm.chi.shopify.com"
  role :uploader, "hadoop-misc4.chi.shopify.com"
end

namespace :deploy do
  task :cleanup do
    count = fetch(:keep_releases, 5).to_i
    run "ls -1dt /u/apps/spark/releases/* | tail -n +#{count + 1} | xargs rm -rf"
  end

  task :upload_to_hdfs, :roles => :uploader, :on_no_matching_servers => :continue do
    run "hdfs dfs -copyFromLocal -f #{release_path}/lib/spark-assembly-*.jar #{fetch(:spark_jar_path)}/spark-assembly-#{fetch(:sha)}.jar"
    run "hdfs dfs -copyFromLocal -f #{release_path}/python/lib/pyspark.zip #{fetch(:spark_jar_path)}/pyspark-#{fetch(:sha)}.zip"
    run "hdfs dfs -copyFromLocal -f #{release_path}/python/lib/py4j-*.zip #{fetch(:spark_jar_path)}/py4j-#{fetch(:sha)}.zip"
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
  before 'deploy:symlink_current', 'deploy:symlink_shared'
  before 'deploy:test_spark_jar', 'deploy:initialize_variables'
  before 'deploy:upload_to_hdfs', 'deploy:initialize_variables'
  after 'deploy:unpack', 'deploy:upload_to_hdfs'
  after 'deploy:restart', 'deploy:cleanup'
  after 'deploy:cleanup', 'deploy:remind_us_to_update_starscream'
end
