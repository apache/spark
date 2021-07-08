# jenkins-infra

This is a rough skeleton of the ansible used to deploy RISELab/Apache Spark Jenkins build workers on Ubuntu 20LTS.

WARNING:  this will not work "directly out of the box" and will need to be tweaked to work on any ubuntu servers you might want to try this on.

### deploy a new worker node
#### TL;DR:
all of the configs for the workers live in roles/common/... and roles/jenkins-worker...

#### prereqs:
* fresh install of ubuntu 20
* a service account w/sudo
* python 3, ansible, ansible-playbook installed locally
* add hostname(s) to the `hosts` file
* add this to your `~/.ansible.cfg`:
```[defaults] host_key_checking = False```

#### fire ansible cannon!
`ansible-playbook -u <service account> deploy-jenkins-worker.yml -i <ansible-style-hosts-file> -k -b -K` 

tips:
* if you are installing more than a few workers, it's best to run the playbook on smaller (2-3) batches at a time.  this way it's easier to track down errors, as ansible is very noisy.
* when you encounter an error, you should comment out any previously-run plays and tasks.  this saves time when debugging, and let's you easily track where you are in the process.
* `apt-get remove <application_name>` and `apt-get purge <package-name>` are your friends
