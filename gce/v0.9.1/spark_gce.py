#!/usr/bin/env python

###
# This script sets up a Spark cluster on Google Compute Engine
###

from __future__ import with_statement

import logging
import os
import pipes
import random
import shutil
import subprocess
import sys
import tempfile
import time
import commands
import urllib2
from optparse import OptionParser
from sys import stderr
import shlex
import getpass
import threading

###
# Make sure gcutil is installed and authenticated 
# Usage: spark_gce.py <project> <no-slaves> <slave-type> <master-type> <identity-file> <zone> <cluster-name>
# Usage: spark_gce.py <project> <cluster-name> destroy
###

identity_file = ""
slave_no = ""
slave_type = ""
master_type = ""
zone = ""
cluster_name = ""
username = ""
project = ""


def read_args():

	global identity_file	
	global slave_no	
	global slave_type	
	global master_type
	global zone
	global cluster_name
	global username
	global project

	if len(sys.argv) == 8:
		project = sys.argv[1]
		slave_no = int(sys.argv[2])
		slave_type = sys.argv[3]
		master_type = sys.argv[4]
		identity_file = sys.argv[5]
		zone = sys.argv[6]
		cluster_name = sys.argv[7]
		username = getpass.getuser()

	elif len(sys.argv) == 4 and sys.argv[3].lower() == "destroy":

		print 'Destroying cluster ' + sys.argv[2]

		project = sys.argv[1]
		cluster_name = sys.argv[2]
		try:

			command = 'gcutil --project=' + project + ' listinstances --columns=name,external-ip --format=csv'
			output = subprocess.check_output(command, shell=True)
			output = output.split("\n")
	
			master_nodes=[]
			slave_nodes=[]

			for line in output:
				if len(line) >= 5:
					host_name = line.split(",")[0]
					host_ip = line.split(",")[1]
					if host_name == cluster_name + '-master':
						command = 'gcutil deleteinstance ' + host_name + ' --project=' + project
						command = shlex.split(command)		
						subprocess.call(command)
					elif cluster_name + '-slave' in host_name:
						command = 'gcutil deleteinstance ' + host_name + ' --project=' + project
						command = shlex.split(command)		
						subprocess.call(command)					

		except:
			print "Failed to Delete instances"
			sys.exit(1)

		sys.exit(0)

	else:
		print '# Usage: spark_gce.py <project> <no-slaves> <slave-type> <master-type> <identity-file> <zone> <cluster-name>'
		print '# Usage: spark_gce.py <project> <cluster-name> destroy'
		sys.exit(0)

		

def setup_network():

	print '[ Setting up Network & Firewall Entries ]'	

	try:
		command = 'gcutil addnetwork ' + cluster_name + '-network --project=' + project
		command = shlex.split(command)		
		subprocess.call(command)

		'''
		command = 'gcutil addfirewall --allowed :22 --project '+ project + ' ssh --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)

		command = 'gcutil addfirewall --allowed :8080 --project '+ project + ' spark-webui --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)
		command = 'gcutil addfirewall --allowed :8081 --project '+ project + ' spark-webui2 --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)
		command = 'gcutil addfirewall --allowed :19999 --project '+ project + ' rpc1 --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)
		command = 'gcutil addfirewall --allowed :50030 --project '+ project + ' rpc2 --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)
		command = 'gcutil addfirewall --allowed :50070 --project '+ project + ' rpc3 --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)
		command = 'gcutil addfirewall --allowed :60070 --project '+ project + ' rpc4 --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)
		command = 'gcutil addfirewall --allowed :4040 --project '+ project + ' app-ui --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)
		command = 'gcutil addfirewall --allowed :4041 --project '+ project + ' app-ui2 --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)
		command = 'gcutil addfirewall --allowed :4042 --project '+ project + ' app-ui3 --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)
		command = 'gcutil addfirewall --allowed :4043 --project '+ project + ' app-ui4 --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)
		command = 'gcutil addfirewall --allowed :4044 --project '+ project + ' app-ui5 --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)
		command = 'gcutil addfirewall --allowed :4045 --project '+ project + ' app-ui6 --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)
		command = 'gcutil addfirewall --allowed :10000 --project '+ project + ' shark-server --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)
		command = 'gcutil addfirewall --allowed :50060 --project '+ project + ' rpc5 --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)
		command = 'gcutil addfirewall --allowed :50075 --project '+ project + ' rpc6 --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)
		command = 'gcutil addfirewall --allowed :60060 --project '+ project + ' rpc7 --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)
		command = 'gcutil addfirewall --allowed :60075 --project '+ project + ' rpc8 --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)

		'''

		#Uncomment the above and comment the below section if you don't want to open all ports for public.
		command = 'gcutil addfirewall --allowed tcp:1-65535 --project '+ project + ' all-tcp-open --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)
		command = 'gcutil addfirewall --allowed udp:1-65535 --project '+ project + ' all-udp-open --network ' + cluster_name +'-network'
		command = shlex.split(command)		
		subprocess.call(command)
		
		
	except OSError:
		print "Failed to setup Network & Firewall. Exiting.."
		sys.exit(1)

			
def launch_master():

	print '[ Launching Master ]'
	command = 'gcutil --service_version="v1" --project="' + project + '" addinstance "' + cluster_name + '-master" --zone="' + zone + '" --machine_type="' + master_type + '" --network="' + cluster_name + '-network" --external_ip_address="ephemeral" --service_account_scopes="https://www.googleapis.com/auth/userinfo.email,https://www.googleapis.com/auth/compute,https://www.googleapis.com/auth/devstorage.full_control" --image="https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-6-v20140415" --persistent_boot_disk="true" --auto_delete_boot_disk="false"'

	command = shlex.split(command)		
	subprocess.call(command)


def launch_slaves():
	
	print '[ Launching Slaves ]'

	for s_id in range(1,slave_no+1):
		command = 'gcutil --service_version="v1" --project="' + project + '" addinstance "' + cluster_name + '-slave' + str(s_id) + '" --zone="' + zone + '" --machine_type="' + slave_type + '" --network="' + cluster_name + '-network" --external_ip_address="ephemeral" --service_account_scopes="https://www.googleapis.com/auth/userinfo.email,https://www.googleapis.com/auth/compute,https://www.googleapis.com/auth/devstorage.full_control" --image="https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-6-v20140415" --persistent_boot_disk="true" --auto_delete_boot_disk="false"'
		command = shlex.split(command)		
		subprocess.call(command)
		
def launch_cluster():
	
	print '[ Creating the Cluster ]'

	setup_network()	

	launch_master()

	launch_slaves()
		

def check_gcutils():
	
	myexec = "gcutil"
	print '[ Verifying gcutil ]'
	try:
		subprocess.call([myexec, 'whoami'])
		
	except OSError:
		print "%s executable not found. \n# Make sure gcutil is installed and authenticated\nPlease follow https://developers.google.com/compute/docs/gcutil/" % myexec
		sys.exit(1)

def get_cluster_ips():
	
	command = 'gcutil --project=' + project + ' listinstances --columns=name,external-ip --format=csv'
	output = subprocess.check_output(command, shell=True)
	output = output.split("\n")
	
	master_nodes=[]
	slave_nodes=[]

	for line in output:
		if len(line) >= 5:
			host_name = line.split(",")[0]
			host_ip = line.split(",")[1]
			if host_name == cluster_name + '-master':
				master_nodes.append(host_ip)
			elif cluster_name + '-slave' in host_name:
				slave_nodes.append(host_ip)

	# Return all the instances
	return (master_nodes, slave_nodes)

def enable_sudo(master,command):
	
	os.system("ssh -i " + identity_file + " -t -o 'UserKnownHostsFile=/dev/null' -o 'CheckHostIP=no' -o 'StrictHostKeyChecking no' "+ username + "@" + master + " '" + command + "'")

def ssh_thread(host,command):

	enable_sudo(host,command)
	
def install_java(master_nodes,slave_nodes):

	print '[ Installing Java 1.7.0 and Development Tools on All machines]'
	master = master_nodes[0]
	
	master_thread = threading.Thread(target=ssh_thread, args=(master,"sudo yum install -y java-1.7.0-openjdk > /dev/null 2>&1;sudo yum install -y java-1.7.0-openjdk-devel > /dev/null 2>&1;sudo yum groupinstall \'Development Tools\' -y > /dev/null 2>&1"))
	master_thread.start()
	
	#ssh_thread(master,"sudo yum install -y java-1.7.0-openjdk")
	for slave in slave_nodes:
		
		slave_thread = threading.Thread(target=ssh_thread, args=(slave,"sudo yum install -y java-1.7.0-openjdk > /dev/null 2>&1;sudo yum install -y java-1.7.0-openjdk-devel > /dev/null 2>&1;sudo yum groupinstall \'Development Tools\' -y > /dev/null 2>&1"))
		slave_thread.start()
		
		#ssh_thread(slave,"sudo yum install -y java-1.7.0-openjdk")
		
	slave_thread.join()
	master_thread.join()


def ssh_command(host,command):
	
	#print "ssh -i " + identity_file + " -o 'UserKnownHostsFile=/dev/null' -o 'CheckHostIP=no' -o 'StrictHostKeyChecking no' "+ username + "@" + host + " '" + command + "'"
	commands.getstatusoutput("ssh -i " + identity_file + " -o 'UserKnownHostsFile=/dev/null' -o 'CheckHostIP=no' -o 'StrictHostKeyChecking no' "+ username + "@" + host + " '" + command + "'" )
	
	
def deploy_keys(master_nodes,slave_nodes):

	print '[ Generating SSH Keys on Master ]'
	key_file = os.path.basename(identity_file)
	master = master_nodes[0]
	ssh_command(master,"ssh-keygen -q -t rsa -N \"\" -f ~/.ssh/id_rsa")
	ssh_command(master,"cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys")
	os.system("scp -i " + identity_file + " -oUserKnownHostsFile=/dev/null -oCheckHostIP=no -oStrictHostKeyChecking=no -o 'StrictHostKeyChecking no' "+ identity_file + " " + username + "@" + master + ":")
	ssh_command(master,"chmod 600 " + key_file)
	ssh_command(master,"tar czf .ssh.tgz .ssh")
	
	ssh_command(master,"ssh-keyscan -H $(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1) >> ~/.ssh/known_hosts")
	ssh_command(master,"ssh-keyscan -H $(cat /etc/hosts | grep $(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1) | cut -d\" \" -f2) >> ~/.ssh/known_hosts")
		
	print '[ Transfering SSH keys to slaves ]'
	for slave in slave_nodes:
		print commands.getstatusoutput("ssh -i " + identity_file + " -oUserKnownHostsFile=/dev/null -oCheckHostIP=no -oStrictHostKeyChecking=no " + username + "@" + master + " 'scp -i " + key_file + " -oStrictHostKeyChecking=no .ssh.tgz " + username +"@" + slave  + ":'")
		ssh_command(slave,"tar xzf .ssh.tgz")
		ssh_command(master,"ssh-keyscan -H " + slave + " >> ~/.ssh/known_hosts")
		ssh_command(slave,"ssh-keyscan -H $(cat /etc/hosts | grep $(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1) | cut -d\" \" -f2) >> ~/.ssh/known_hosts")
		ssh_command(slave,"ssh-keyscan -H $(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1) >> ~/.ssh/known_hosts")



def attach_drive(master_nodes,slave_nodes):

	print '[ Adding new 500GB drive on Master ]'
	master = master_nodes[0]

	command='gcutil --service_version="v1" --project="' + project + '" adddisk "' + cluster_name + '-m-disk" --size_gb="500" --zone="' + zone + '"'
	command = shlex.split(command)		
	subprocess.call(command)
	
	command = 'gcutil --project='+ project +' attachdisk --zone=' + zone +' --disk=' + cluster_name + '-m-disk ' + cluster_name + '-master'
	command = shlex.split(command)		
	subprocess.call(command)

	print '[ Formating Disk on Master ]'

	master_thread = threading.Thread(target=ssh_thread, args=(master,"sudo mkfs.ext3 /dev/disk/by-id/google-"+ cluster_name + "-m-disk " + " -F < /dev/null > /dev/null 2>&1"))
	master_thread.start()

	print '[ Adding new 500GB drive on Slaves ]'

	i = 1
	for slave in slave_nodes:

		master = slave

		command='gcutil --service_version="v1" --project="' + project + '" adddisk "' + cluster_name + '-s' + str(i) + '-disk" --size_gb="500" --zone="' + zone + '"'
		command = shlex.split(command)		
		subprocess.call(command)
	
		command = 'gcutil --project='+ project +' attachdisk --zone=' + zone +' --disk=' + cluster_name + '-s' + str(i) + '-disk ' + cluster_name + '-slave' +  str(i)
		command = shlex.split(command)		
		subprocess.call(command)
		print '[ Formating Disk on Slave :' + slave + ' ]'
		slave_thread = threading.Thread(target=ssh_thread, args=(slave,"sudo mkfs.ext3 /dev/disk/by-id/google-" + cluster_name + "-s" + str(i) + "-disk -F < /dev/null > /dev/null 2>&1"))
		slave_thread.start()
		i=i+1

	slave_thread.join()
	master_thread.join()

	print '[ Mounting new Volume ]'
	enable_sudo(master_nodes[0],"sudo mount /dev/disk/by-id/google-"+ cluster_name + "-m-disk /mnt")
	enable_sudo(master_nodes[0],"sudo chown " + username + ":" + username + " /mnt")
	i=1
	for slave in slave_nodes:			
		enable_sudo(slave,"sudo mount /dev/disk/by-id/google-"+ cluster_name + "-s" + str(i) +"-disk /mnt")
		enable_sudo(slave,"sudo chown " + username + ":" + username + " /mnt")
		i=i+1

	print '\n\n[ All volumns mounted, will be available at /mnt ]'

def setup_spark(master_nodes,slave_nodes):
	
	print '\n\n[ Downloading Spark Binaries ]'
	
	master = master_nodes[0]

	ssh_command(master,"rm -fr spark-gce;mkdir spark-gce")
	ssh_command(master,"cd spark-gce;wget http://d3kbcqa49mib13.cloudfront.net/spark-0.9.1-bin-hadoop2.tgz")
	ssh_command(master,"cd spark-gce;wget http://www.scala-lang.org/files/archive/scala-2.10.3.tgz")
	ssh_command(master,"cd spark-gce;tar zxf spark-0.9.1-bin-hadoop2.tgz;rm spark-0.9.1-bin-hadoop2.tgz;ln -s spark-0.9.1-bin-hadoop2 spark")
	ssh_command(master,"cd spark-gce;tar zxf scala-2.10.3.tgz;rm scala-2.10.3.tgz;ln -s scala-2.10.3 scala")	

	print '[ Updating Spark Configurations ]'
	ssh_command(master,"cd spark-gce;cd spark/conf;cp spark-env.sh.template spark-env.sh")
	ssh_command(master,"cd spark-gce;cd spark/conf;echo 'export SCALA_HOME=\"/home/`whoami`/spark-gce/scala\"' >> spark-env.sh")
	ssh_command(master,"cd spark-gce;cd spark/conf;echo 'export SPARK_MEM=2454m' >> spark-env.sh")
	ssh_command(master,"cd spark-gce;cd spark/conf;echo \"SPARK_JAVA_OPTS+=\\\" -Dspark.local.dir=/mnt/spark \\\"\" >> spark-env.sh")	
	ssh_command(master,"cd spark-gce;cd spark/conf;echo 'export SPARK_JAVA_OPTS' >> spark-env.sh")
	ssh_command(master,"cd spark-gce;cd spark/conf;echo 'export SPARK_MASTER_IP=PUT_MASTER_IP_HERE' >> spark-env.sh")
	ssh_command(master,"cd spark-gce;cd spark/conf;echo 'export MASTER=spark://PUT_MASTER_IP_HERE:7077' >> spark-env.sh")
	ssh_command(master,"cd spark-gce;cd spark/conf;echo 'export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.55.x86_64' >> spark-env.sh")
	
	ssh_command(master,"cd spark-gce;cd spark/conf;rm slaves")
	for slave in slave_nodes:
		ssh_command(master,"echo " + slave + " >> spark-gce/spark/conf/slaves")

	
	ssh_command(master,"sed -i \"s/PUT_MASTER_IP_HERE/$(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1)/g\" spark-gce/spark/conf/spark-env.sh")
	
	ssh_command(master,"chmod +x spark-gce/spark/conf/spark-env.sh")

	print '[ Rsyncing Spark to all slaves ]'

	for slave in slave_nodes:
		ssh_command(master,"rsync -za /home/" + username + "/spark-gce " + slave + ":")
		ssh_command(slave,"mkdir /mnt/spark")
	
	ssh_command(master,"mkdir /mnt/spark")
	print '[ Starting Spark Cluster ]'
	ssh_command(master,"spark-gce/spark/sbin/start-all.sh")
	
	setup_shark(master_nodes,slave_nodes)
	
	setup_hadoop(master_nodes,slave_nodes)

	print "\n\nSpark Cluster Started, WebUI available at : http://" + master + ":8080"
	

def setup_hadoop(master_nodes,slave_nodes):

	master = master_nodes[0]
	print '[ Downloading hadoop ]'
	
	ssh_command(master,"cd spark-gce;wget http://archive.cloudera.com/cdh5/cdh/5/hadoop-2.2.0-cdh5.0.0-beta-1.tar.gz")
	ssh_command(master,"cd spark-gce;tar zxf hadoop-2.2.0-cdh5.0.0-beta-1.tar.gz;rm hadoop-2.2.0-cdh5.0.0-beta-1.tar.gz;ln -s hadoop-2.2.0-cdh5.0.0-beta-1 hadoop")
	
	print '[ Configuring Hadoop ]'
	
	#Configure .bashrc
	ssh_command(master,"echo '\#HADOOP_CONFS' >> .bashrc")
	ssh_command(master,"echo 'export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.55.x86_64' >> .bashrc")
	ssh_command(master,"echo 'export HADOOP_INSTALL=/home/`whoami`/spark-gce/hadoop' >> .bashrc")
	ssh_command(master,"echo 'export PATH=$PATH:\$HADOOP_INSTALL/bin' >> .bashrc")
	ssh_command(master,"echo 'export PATH=$PATH:\$HADOOP_INSTALL/sbin' >> .bashrc")
	ssh_command(master,"echo 'export HADOOP_MAPRED_HOME=\$HADOOP_INSTALL' >> .bashrc")
	ssh_command(master,"echo 'export HADOOP_COMMON_HOME=\$HADOOP_INSTALL' >> .bashrc")
	ssh_command(master,"echo 'export HADOOP_HDFS_HOME=\$HADOOP_INSTALL' >> .bashrc")
	ssh_command(master,"echo 'export YARN_HOME=\$HADOOP_INSTALL' >> .bashrc")
	
	ssh_command(master,"cd spark-gce/hadoop/etc/hadoop/;cp mapred-site.xml.template mapred-site.xml")
	
	#Generate Confs
	ssh_command(master,"cd spark-gce/hadoop/etc/hadoop/;sed -i \"s/<\/configuration>//g\" core-site.xml")
	ssh_command(master,"cd spark-gce/hadoop/etc/hadoop/;echo \"<property>\" >> core-site.xml;echo \"<name>fs.default.name</name>\" >> core-site.xml;echo \"<value>hdfs://PUT-MASTER-IP:9000</value>\" >> core-site.xml;echo \"</property>\" >> core-site.xml;echo \"</configuration>\" >> core-site.xml")
	#Config masterIP in Core-site
	ssh_command(master,"cd spark-gce/hadoop/etc/hadoop/;sed -i \"s/PUT-MASTER-IP/$(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1)/g\" core-site.xml")
	#Generate hdfs-site.xml
	ssh_command(master,"cd spark-gce/hadoop/etc/hadoop/;sed -i \"s/<\/configuration>//g\" hdfs-site.xml")
	ssh_command(master,"cd spark-gce/hadoop/etc/hadoop/;echo \"<property>\" >> hdfs-site.xml;echo \"<name>dfs.replication</name>\" >> hdfs-site.xml;echo \"<value>1</value>\" >> hdfs-site.xml;echo \"</property>\" >> hdfs-site.xml;echo \"<property>\" >> hdfs-site.xml;echo \"<name>dfs.namenode.name.dir</name>\" >>hdfs-site.xml;echo \"<value>/mnt/hadoop/hdfs/namenode</value>\" >> hdfs-site.xml;echo \"</property>\" >> hdfs-site.xml;echo \"<property>\" >> hdfs-site.xml;echo \"<name>dfs.datanode.data.dir</name>\" >> hdfs-site.xml;echo \"<value>/mnt/hadoop/hdfs/datanode</value>\" >> hdfs-site.xml;echo \"</property>\" >> hdfs-site.xml;echo \"</configuration>\" >> hdfs-site.xml")

	#Generate mapred-site.xml
	ssh_command(master,"cd spark-gce/hadoop/etc/hadoop/;sed -i \"s/<\/configuration>//g\" mapred-site.xml")
	ssh_command(master,"cd spark-gce/hadoop/etc/hadoop/;echo \"<property>\" >> mapred-site.xml;echo \"<name>mapreduce.framework.name</name>\" >> mapred-site.xml;echo \"<value>yarn</value>\" >> mapred-site.xml;echo \"</property>\" >> mapred-site.xml;echo \"</configuration>\" >> mapred-site.xml")

	#Generate yarn-site.xml
	ssh_command(master,"cd spark-gce/hadoop/etc/hadoop/;sed -i \"s/<\/configuration>//g\" yarn-site.xml")
	ssh_command(master,"cd spark-gce/hadoop/etc/hadoop/;echo \"<property>\" >> yarn-site.xml;echo \"<name>yarn.nodemanager.aux-services</name>\" >> yarn-site.xml;echo \"<value>mapreduce_shuffle</value>\" >> yarn-site.xml;echo \"</property>\" >> yarn-site.xml;echo \"<property>\" >> yarn-site.xml;echo \"<name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>\" >> yarn-site.xml;echo  \"<value>org.apache.hadoop.mapred.ShuffleHandler</value>\" >> yarn-site.xml;echo \"</property>\" >> yarn-site.xml;echo \"</configuration>\" >> yarn-site.xml")
	

	#Create data/node dirs
	ssh_command(master,"mkdir -p /mnt/hadoop/hdfs/namenode;mkdir -p /mnt/hadoop/hdfs/datanode")
	#Config slaves
	ssh_command(master,"cd spark-gce/hadoop/etc/hadoop/;rm slaves")
	for slave in slave_nodes:
		ssh_command(master,"cd spark-gce/hadoop/etc/hadoop/;echo " + slave + " >> slaves")

	print '[ Rsyncing with Slaves ]'
	#Rsync everything
	for slave in slave_nodes:
		ssh_command(master,"rsync -za /home/" + username + "/spark-gce " + slave + ":")
		ssh_command(slave,"mkdir -p /mnt/hadoop/hdfs/namenode;mkdir -p /mnt/hadoop/hdfs/datanode")
		ssh_command(master,"rsync -za /home/" + username + "/.bashrc " + slave + ":")

	print '[ Formating namenode ]'
	#Format namenode
	ssh_command(master,"spark-gce/hadoop/bin/hdfs namenode -format")
	
	print '[ Starting DFS ]'
	#Start dfs
	ssh_command(master,"spark-gce/hadoop/sbin/start-dfs.sh")

def setup_shark(master_nodes,slave_nodes):

	master = master_nodes[0]
	print '[ Downloading Shark binaries ]'
	
	ssh_command(master,"cd spark-gce;wget https://s3.amazonaws.com/spark-related-packages/shark-0.9.1-bin-hadoop2.tgz")	
	ssh_command(master,"cd spark-gce;tar zxf shark-0.9.1-bin-hadoop2.tgz;rm shark-0.9.1-bin-hadoop2.tgz;ln -s shark-0.9.1-bin-hadoop2 shark")
		
	print '[ Configuring Shark ]'
	ssh_command(master,"cd spark-gce/shark/;echo \"export SHARK_MASTER_MEM=1g\" > conf/shark-env.sh")
	ssh_command(master,"cd spark-gce/shark/;echo \"SPARK_JAVA_OPTS+=\\\" -Dspark.kryoserializer.buffer.mb=10 \\\"\" >> conf/shark-env.sh")
	ssh_command(master,"cd spark-gce/shark/;echo \"export SPARK_JAVA_OPTS\" >> conf/shark-env.sh")	
	ssh_command(master,"cd spark-gce/shark/;echo \"export MASTER=spark://PUT_MASTER_IP_HERE:7077\" >> conf/shark-env.sh")
	ssh_command(master,"cd spark-gce/shark/;echo \"export SPARK_HOME=/home/`whoami`/spark-gce/spark\" >> conf/shark-env.sh")
	ssh_command(master,"mkdir /mnt/tachyon")
	ssh_command(master,"cd spark-gce/shark/;echo \"export TACHYON_MASTER=PUT_MASTER_IP_HERE:19998\" >> conf/shark-env.sh")
	ssh_command(master,"cd spark-gce/shark/;echo \"export TACHYON_WAREHOUSE_PATH=/mnt/tachyon\" >> conf/shark-env.sh")
	ssh_command(master,"cd spark-gce/shark/;echo \"export HADOOP_HOME=/home/`whoami`/spark-gce/hadoop\" >> conf/shark-env.sh")	
	ssh_command(master,"cd spark-gce/shark/;echo \"source /home/`whoami`/spark-gce/spark/conf/spark-env.sh\" >> conf/shark-env.sh")	
	ssh_command(master,"sed -i \"s/PUT_MASTER_IP_HERE/$(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1)/g\" spark-gce/shark/conf/shark-env.sh")

	ssh_command(master,"chmod +x spark-gce/shark/conf/shark-env.sh")
	
	print '[ Rsyncing Shark on slaves ]'
	for slave in slave_nodes:
		ssh_command(master,"rsync -za /home/" + username + "/spark-gce " + slave + ":")

	print '[ Starting Shark Server ]'
	ssh_command(master,"cd spark-gce/shark/;./bin/shark --service sharkserver 10000 > log.txt 2>&1 &")

	
def real_main():

	print "[ Script Started ]"	
	#Read the arguments
	read_args()
	#Make sure gcutil is accessible.
	check_gcutils()

	#Launch the cluster
	launch_cluster()
	sys.stdout.write('\n')
	#Wait some time for machines to bootup
	print '[ Waiting 120 Seconds for Machines to start up ]'
	time.sleep(120)

	#Get Master/Slave IP Addresses
	(master_nodes, slave_nodes) = get_cluster_ips()

	#Install Java and build-essential
	install_java(master_nodes,slave_nodes)
	sys.stdout.write('\n')

	#Generate SSH keys and deploy
	deploy_keys(master_nodes,slave_nodes)

	#Attach a new empty drive and format it
	attach_drive(master_nodes,slave_nodes)
	sys.stdout.write('\n')

	#Set up Spark/Shark/Hadoop
	setup_spark(master_nodes,slave_nodes)


def main():
  try:
    real_main()
  except Exception as e:
    print >> stderr, "\nError:\n", e
    

if __name__ == "__main__":
  
  main()
