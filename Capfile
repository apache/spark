require 'bundler/setup'
require 'capistrano_recipes/deploy/packserv'

set :application, "spark"
role :app, *((4..47).map {|i| "dn%02d.chi.shopify.com" % i } - ["dn41.chi.shopify.com", "dn05.chi.shopify.com"])
role :master, "dn05.chi.shopify.com"
role :code, "hadoop-etl1.chi.shopify.com", "spark-etl1.chi.shopify.com"

namespace :deploy do
  task :setup_spark_paths do
    set :shared_work_path, "#{deploy_to}/shared/work"
    set :shared_logs_path, "#{deploy_to}/shared/log"
    set :shared_conf_path, "#{deploy_to}/shared/conf"
    set :gateway, nil
  end

  task :restart_master, :roles => :master do
    run "sv-sudo restart spark-master"
  end

  task :restart, :roles => :app do
    run "sv-sudo restart spark-worker"
  end

  task :symlink_shared do
      run "ln -nfs #{shared_work_path} #{release_path}/work"
      run "ln -nfs #{shared_logs_path} #{release_path}/logs"
      run "rm -rf #{release_path}/conf && ln -nfs #{shared_conf_path} #{release_path}/conf"
  end

  before 'deploy:restart', 'deploy:symlink_shared', 'deploy:restart_master'
  after  'deploy:initialize_variables', 'deploy:setup_spark_paths'
end
