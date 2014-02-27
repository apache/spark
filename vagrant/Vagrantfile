# -*- mode: ruby -*-
# vi: set ft=ruby :

# Using Vagrant shell scripts
# https://github.com/StanAngeloff/vagrant-shell-scripts
require File.join(File.dirname(__FILE__), 'scripts/vagrant-shell-scripts/vagrant')

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"
Vagrant.require_version ">= 1.4.2"

# the domain of all the nodes
DOMAIN = 'local'

# Ubuntu 12.04.4 LTS (Precise Pangolin) 64 bit
BOX = 'precise64'
BOX_URL = 'http://files.vagrantup.com/precise64.box'

# define all the nodes here.
# :host is the id in Vagrant of the node. It will also be its hostname
# :ip the ip address of the node
# :cpu the number of core for the node
# :ram the amount of RAM allocated to the node in MBytes.
NODES = [
  { :host => 'spark-master', :ip => '192.168.2.10', :cpu => 1, :ram => '3072' },
  { :host => 'spark-worker-1', :ip => '192.168.2.20', :cpu => 1, :ram => '2048' },
  { :host => 'spark-worker-2', :ip => '192.168.2.21', :cpu => 1, :ram => '2048' }
]

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  NODES.each do | node |
    config.vm.define node[:host] do | node_config |
      node_config.vm.box = BOX
      node_config.vm.box_url = BOX_URL

      node_config.vm.hostname = node[:host] + "." + DOMAIN
      node_config.vm.network :private_network, ip: node[:ip]

      # by default, the current folder is shared as /vagrant in the nodes
      # you can also share an outside folder here
      # node_config.vm.synced_folder "shared", "/shared"

      memory = node[:ram] ? node[:ram] : 1024
      cpu = node[:cpu] ? node[:cpu] : 1

      node_config.vm.provider :virtualbox do | vbox |
        vbox.gui = false
        vbox.customize ['modifyvm', :id, '--memory', memory.to_s]
        vbox.customize ['modifyvm', :id, '--cpus', cpu.to_s]

        # fix the connection slow
        # https://github.com/mitchellh/vagrant/issues/1807
        vbox.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
        vbox.customize ["modifyvm", :id, "--natdnsproxy1", "on"]
      end

      node_config.vm.provision :shell do |shell|
        vagrant_shell_scripts_configure(
        shell,
        File.dirname(__FILE__),
        'scripts/setup.sh')
      end
    end
  end
end
