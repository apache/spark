require 'bundler/setup'
require 'capistrano_recipes/deploy/packserv'

set :application, "spark"
set :user, "deploy"
set :shared_work_path, "/u/apps/spark/shared/work"
set :shared_logs_path, "/u/apps/spark/shared/log"
set :shared_conf_path, "/u/apps/spark/shared/conf"
set :gateway, nil

BROKEN = ["dn04.chi.shopify.com", "dn06.chi.shopify.com", "dn07.chi.shopify.com", "dn08.chi.shopify.com", "dn11.chi.shopify.com", "dn14.chi.shopify.com"]

task :production do
  role :app, *((4..47).map {|i| "dn%02d.chi.shopify.com" % i } - (["dn05.chi.shopify.com"] + BROKEN))
  role :master, "dn05.chi.shopify.com"
  role :history, "dn05.chi.shopify.com"
  role :code, "hadoop-etl1.chi.shopify.com", "spark-etl1.chi.shopify.com", "reports-reportify-etl3.chi.shopify.com", "reports-reportify-skydb4.chi.shopify.com", "platfora2.chi.shopify.com"
end

task :staging do
  role :app, "54.197.179.141", "107.21.65.199", "54.166.211.228"
  role :master, "54.167.53.104"
end


namespace :deploy do
  task :restart_master, :roles => :master, :on_no_matching_servers => :continue do
    run "sv-sudo restart spark-master"
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

  before 'deploy:restart', 'deploy:restart_master', 'deploy:restart_history'
  after  'deploy:finalize_update', 'deploy:setup_spark_paths'
end
