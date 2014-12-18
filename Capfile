require 'bundler/setup'
require 'capistrano_recipes/deploy/packserv'

set :application, "spark"
set :user, "deploy"
set :shared_work_path, "/u/apps/spark/shared/work"
set :shared_logs_path, "/u/apps/spark/shared/log"
set :shared_conf_path, "/u/apps/spark/shared/conf"
set :gateway, nil
set :keep_releases, 5

MAINTENANCE = (11..15).map {|i| "dn%02d.chi.shopify.com" % i } # Node is up but should not be part of the production cluster
LOAD_TESTING = (42..44).map {|i| "dn%02d.chi.shopify.com" % i }
DECOMISSIONED = ["dn37.chi.shopify.com", "dn40.chi.shopify.com", "dn09.chi.shopify.com", "dn46.chi.shopify.com", "dn17.chi.shopify.com"] # Node is down don't try to send code
BROKEN = MAINTENANCE + DECOMISSIONED

task :production do
  role :app, *((2..47).map {|i| "dn%02d.chi.shopify.com" % i } - (BROKEN + LOAD_TESTING))
  role :master, "hadoop-rm.chi.shopify.com"
  role :history, "hadoop-rm.chi.shopify.com"
  role :code, "hadoop-etl1.chi.shopify.com", "spark-etl1.chi.shopify.com", "reports-reportify-etl3.chi.shopify.com", "reports-reportify-skydb4.chi.shopify.com", "platfora2.chi.shopify.com", *MAINTENANCE
  role :uploader, "hadoop-etl1.chi.shopify.com"
end

task :load_testing do
  role :app, *((43..44).map {|i| "dn%02d.chi.shopify.com" % i } - BROKEN)
  role :master, "dn42.chi.shopify.com"
end


task :staging do
  role :app, "54.197.179.141", "107.21.65.199", "54.166.211.228"
  role :master, "54.167.53.104"
end

namespace :deploy do
  task :cleanup do
    count = fetch(:keep_releases, 5).to_i
    run "ls -1dt /u/apps/spark/releases/* | tail -n +#{count + 1} | xargs rm -rf"
  end

  task :upload_to_hdfs, :roles => :uploader, :on_no_matching_servers => :continue do
    run "hdfs dfs -copyFromLocal -f /u/apps/spark/current/lib/spark-assembly-1.3.0-SNAPSHOT-hadoop2.5.0.jar hdfs://nn01.chi.shopify.com/user/sparkles/spark-assembly-#{`git rev-parse master`.gsub(/\s/,'')}.jar"
  end

  task :prevent_gateway do
    set :gateway, nil
  end

  task :restart_master, :roles => :master, :on_no_matching_servers => :continue do
    run "sv-sudo restart spark-master"
    run "sv-sudo restart spark-master-sha-server"
  end

  task :restart_history, :roles => :history, :on_no_matching_servers => :continue do
    run "sv-sudo restart spark-history"
  end

  task :restart, :roles => :app do
    run "sv-sudo restart spark-worker"
  end

  task :symlink_shared do
    run "ln -nfs #{shared_work_path} #{release_path}/work"
    run "ln -nfs #{shared_logs_path} #{release_path}/logs"
    run "rm -rf #{release_path}/conf && ln -nfs #{shared_conf_path} #{release_path}/conf"
  end

  after 'deploy:initialize_variables', 'deploy:prevent_gateway' # capistrano recipes packserv deploy always uses a gateway
  before  'deploy:symlink_current', 'deploy:symlink_shared'
  before 'deploy:restart', 'deploy:restart_master', 'deploy:restart_history'
  after  'deploy:download', 'deploy:upload_to_hdfs'
  after 'deploy:restart', 'deploy:cleanup'
end
