require 'bundler/setup'
require 'capistrano_recipes/deploy/packserv'

set :application, "spark"
set :user, "deploy"
set :shared_work_path, "/u/apps/spark/shared/work"
set :shared_logs_path, "/u/apps/spark/shared/log"
set :shared_conf_path, "/u/apps/spark/shared/conf"
set :gateway, nil
set :keep_releases, 5

DATANODES = (2..47).map {|i| "dn%02d.chi.shopify.com" % i }
OTHERNODES = ["hadoop-etl1.chi.shopify.com", "spark-etl1.chi.shopify.com", "reports-reportify-etl3.chi.shopify.com", "reports-reportify-skydb4.chi.shopify.com", "platfora2.chi.shopify.com"]
BROKEN = ["dn16.chi.shopify.com"] # Node is down don't try to send code

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

  task :upload_to_hdfs, :roles => :uploader, :on_no_matching_servers => :continue do
    run "hdfs dfs -copyFromLocal -f /u/apps/spark/current/lib/spark-assembly-*.jar hdfs://nn01.chi.shopify.com/user/sparkles/spark-assembly-latest.jar"
  end

  task :prevent_gateway do
    set :gateway, nil
  end

  task :symlink_shared do
    run "ln -nfs #{shared_work_path} #{release_path}/work"
    run "ln -nfs #{shared_logs_path} #{release_path}/logs"
    run "rm -rf #{release_path}/conf && ln -nfs #{shared_conf_path} #{release_path}/conf"
  end

  task :restart do
  end

  after 'deploy:initialize_variables', 'deploy:prevent_gateway' # capistrano recipes packserv deploy always uses a gateway
  before  'deploy:symlink_current', 'deploy:symlink_shared'
  after  'deploy:download', 'deploy:upload_to_hdfs'
  after 'deploy:restart', 'deploy:cleanup'
end
