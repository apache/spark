# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import with_statement

import ConfigParser
from hadoop.cloud import VERSION
from hadoop.cloud.cluster import get_cluster
from hadoop.cloud.service import get_service
from hadoop.cloud.service import InstanceTemplate
from hadoop.cloud.service import NAMENODE
from hadoop.cloud.service import SECONDARY_NAMENODE
from hadoop.cloud.service import JOBTRACKER
from hadoop.cloud.service import DATANODE
from hadoop.cloud.service import TASKTRACKER
from hadoop.cloud.util import merge_config_with_options
from hadoop.cloud.util import xstr
import logging
from optparse import OptionParser
from optparse import make_option
import os
import sys

DEFAULT_SERVICE_NAME = 'hadoop'
DEFAULT_CLOUD_PROVIDER = 'ec2'

DEFAULT_CONFIG_DIR_NAME = '.hadoop-cloud'
DEFAULT_CONFIG_DIR = os.path.join(os.environ['HOME'], DEFAULT_CONFIG_DIR_NAME)
CONFIG_FILENAME = 'clusters.cfg'

CONFIG_DIR_OPTION = \
  make_option("--config-dir", metavar="CONFIG-DIR",
    help="The configuration directory.")

PROVIDER_OPTION = \
  make_option("--cloud-provider", metavar="PROVIDER",
    help="The cloud provider, e.g. 'ec2' for Amazon EC2.")

BASIC_OPTIONS = [
  CONFIG_DIR_OPTION,
  PROVIDER_OPTION,
]

LAUNCH_OPTIONS = [
  CONFIG_DIR_OPTION,
  PROVIDER_OPTION,
  make_option("-a", "--ami", metavar="AMI",
    help="The AMI ID of the image to launch. (Amazon EC2 only. Deprecated, use \
--image-id.)"),
  make_option("-e", "--env", metavar="ENV", action="append",
    help="An environment variable to pass to instances. \
(May be specified multiple times.)"),
  make_option("-f", "--user-data-file", metavar="URL",
    help="The URL of the file containing user data to be made available to \
instances."),
  make_option("--image-id", metavar="ID",
    help="The ID of the image to launch."),
  make_option("-k", "--key-name", metavar="KEY-PAIR",
    help="The key pair to use when launching instances. (Amazon EC2 only.)"),
  make_option("-p", "--user-packages", metavar="PACKAGES",
    help="A space-separated list of packages to install on instances on start \
up."),
  make_option("-t", "--instance-type", metavar="TYPE",
    help="The type of instance to be launched. One of m1.small, m1.large, \
m1.xlarge, c1.medium, or c1.xlarge."),
  make_option("-z", "--availability-zone", metavar="ZONE",
    help="The availability zone to run the instances in."),
  make_option("--auto-shutdown", metavar="TIMEOUT_MINUTES",
    help="The time in minutes after launch when an instance will be \
automatically shut down."),
  make_option("--client-cidr", metavar="CIDR", action="append",
    help="The CIDR of the client, which is used to allow access through the \
firewall to the master node. (May be specified multiple times.)"),
  make_option("--security-group", metavar="SECURITY_GROUP", action="append",
    default=[], help="Additional security groups within which the instances \
should be run. (Amazon EC2 only.) (May be specified multiple times.)"),
  make_option("--public-key", metavar="FILE",
    help="The public key to authorize on launching instances. (Non-EC2 \
providers only.)"),
]

SNAPSHOT_OPTIONS = [
  CONFIG_DIR_OPTION,
  PROVIDER_OPTION,
  make_option("-k", "--key-name", metavar="KEY-PAIR",
    help="The key pair to use when launching instances."),
  make_option("-z", "--availability-zone", metavar="ZONE",
    help="The availability zone to run the instances in."),
  make_option("--ssh-options", metavar="SSH-OPTIONS",
    help="SSH options to use."),
]

PLACEMENT_OPTIONS = [
  CONFIG_DIR_OPTION,
  PROVIDER_OPTION,
  make_option("-z", "--availability-zone", metavar="ZONE",
    help="The availability zone to run the instances in."),
]

FORCE_OPTIONS = [
  CONFIG_DIR_OPTION,
  PROVIDER_OPTION,
  make_option("--force", action="store_true", default=False,
  help="Do not ask for confirmation."),
]

SSH_OPTIONS = [
  CONFIG_DIR_OPTION,
  PROVIDER_OPTION,
  make_option("--ssh-options", metavar="SSH-OPTIONS",
    help="SSH options to use."),
]

def print_usage(script):
  print """Usage: %(script)s COMMAND [OPTIONS]
where COMMAND and [OPTIONS] may be one of:
  list [CLUSTER]                      list all running Hadoop clusters
                                        or instances in CLUSTER
  launch-master CLUSTER               launch or find a master in CLUSTER
  launch-slaves CLUSTER NUM_SLAVES    launch NUM_SLAVES slaves in CLUSTER
  launch-cluster CLUSTER (NUM_SLAVES| launch a master and NUM_SLAVES slaves or
    N ROLE [N ROLE ...])                N instances in ROLE in CLUSTER
  create-formatted-snapshot CLUSTER   create an empty, formatted snapshot of
    SIZE                                size SIZE GiB
  list-storage CLUSTER                list storage volumes for CLUSTER
  create-storage CLUSTER ROLE         create volumes for NUM_INSTANCES instances
    NUM_INSTANCES SPEC_FILE             in ROLE for CLUSTER, using SPEC_FILE
  attach-storage ROLE                 attach storage volumes for ROLE to CLUSTER
  login CLUSTER                       log in to the master in CLUSTER over SSH
  proxy CLUSTER                       start a SOCKS proxy on localhost into the
                                        CLUSTER
  push CLUSTER FILE                   scp FILE to the master in CLUSTER
  exec CLUSTER CMD                    execute CMD on the master in CLUSTER
  terminate-cluster CLUSTER           terminate all instances in CLUSTER
  delete-cluster CLUSTER              delete the group information for CLUSTER
  delete-storage CLUSTER              delete all storage volumes for CLUSTER
  update-slaves-file CLUSTER          update the slaves file on the CLUSTER
                                        master

Use %(script)s COMMAND --help to see additional options for specific commands.
""" % locals()

def print_deprecation(script, replacement):
  print "Deprecated. Use '%(script)s %(replacement)s'." % locals()

def parse_options_and_config(command, option_list=[], extra_arguments=(),
                             unbounded_args=False):
  """
  Parse the arguments to command using the given option list, and combine with
  any configuration parameters.

  If unbounded_args is true then there must be at least as many extra arguments
  as specified by extra_arguments (the first argument is always CLUSTER).
  Otherwise there must be exactly the same number of arguments as
  extra_arguments.
  """
  expected_arguments = ["CLUSTER",]
  expected_arguments.extend(extra_arguments)
  (options_dict, args) = parse_options(command, option_list, expected_arguments,
                                       unbounded_args)

  config_dir = get_config_dir(options_dict)
  config_files = [os.path.join(config_dir, CONFIG_FILENAME)]
  if 'config_dir' not in options_dict:
    # if config_dir not set, then also search in current directory
    config_files.insert(0, CONFIG_FILENAME)

  config = ConfigParser.ConfigParser()
  read_files = config.read(config_files)
  logging.debug("Read %d configuration files: %s", len(read_files),
                ", ".join(read_files))
  cluster_name = args[0]
  opt = merge_config_with_options(cluster_name, config, options_dict)
  logging.debug("Options: %s", str(opt))
  service_name = get_service_name(opt)
  cloud_provider = get_cloud_provider(opt)
  cluster = get_cluster(cloud_provider)(cluster_name, config_dir)
  service = get_service(service_name, cloud_provider)(cluster)
  return (opt, args, service)

def parse_options(command, option_list=[], expected_arguments=(),
                  unbounded_args=False):
  """
  Parse the arguments to command using the given option list.

  If unbounded_args is true then there must be at least as many extra arguments
  as specified by extra_arguments (the first argument is always CLUSTER).
  Otherwise there must be exactly the same number of arguments as
  extra_arguments.
  """

  config_file_name = "%s/%s" % (DEFAULT_CONFIG_DIR_NAME, CONFIG_FILENAME)
  usage = """%%prog %s [options] %s

Options may also be specified in a configuration file called
%s located in the user's home directory.
Options specified on the command line take precedence over any in the
configuration file.""" % (command, " ".join(expected_arguments),
                          config_file_name)
  parser = OptionParser(usage=usage, version="%%prog %s" % VERSION,
                        option_list=option_list)
  parser.disable_interspersed_args()
  (options, args) = parser.parse_args(sys.argv[2:])
  if unbounded_args:
    if len(args) < len(expected_arguments):
      parser.error("incorrect number of arguments")
  elif len(args) != len(expected_arguments):
    parser.error("incorrect number of arguments")
  return (vars(options), args)

def get_config_dir(options_dict):
  config_dir = options_dict.get('config_dir')
  if not config_dir:
    config_dir = DEFAULT_CONFIG_DIR
  return config_dir

def get_service_name(options_dict):
  service_name = options_dict.get("service", None)
  if service_name is None:
    service_name = DEFAULT_SERVICE_NAME
  return service_name

def get_cloud_provider(options_dict):
  provider = options_dict.get("cloud_provider", None)
  if provider is None:
    provider = DEFAULT_CLOUD_PROVIDER
  return provider

def check_options_set(options, option_names):
  for option_name in option_names:
    if options.get(option_name) is None:
      print "Option '%s' is missing. Aborting." % option_name
      sys.exit(1)

def check_launch_options_set(cluster, options):
  if cluster.get_provider_code() == 'ec2':
    if options.get('ami') is None and options.get('image_id') is None:
      print "One of ami or image_id must be specified. Aborting."
      sys.exit(1)
    check_options_set(options, ['key_name'])
  else:
    check_options_set(options, ['image_id', 'public_key'])

def get_image_id(cluster, options):
  if cluster.get_provider_code() == 'ec2':
    return options.get('image_id', options.get('ami'))
  else:
    return options.get('image_id')

def main():
  # Use HADOOP_CLOUD_LOGGING_LEVEL=DEBUG to enable debugging output.
  logging.basicConfig(level=getattr(logging,
                                    os.getenv("HADOOP_CLOUD_LOGGING_LEVEL",
                                              "INFO")))

  if len(sys.argv) < 2:
    print_usage(sys.argv[0])
    sys.exit(1)

  command = sys.argv[1]

  if command == 'list':
    (opt, args) = parse_options(command, BASIC_OPTIONS, unbounded_args=True)
    if len(args) == 0:
      service_name = get_service_name(opt)
      cloud_provider = get_cloud_provider(opt)
      service = get_service(service_name, cloud_provider)(None)
      service.list_all(cloud_provider)
    else:
      (opt, args, service) = parse_options_and_config(command, BASIC_OPTIONS)
      service.list()

  elif command == 'launch-master':
    (opt, args, service) = parse_options_and_config(command, LAUNCH_OPTIONS)
    check_launch_options_set(service.cluster, opt)
    config_dir = get_config_dir(opt)
    template = InstanceTemplate((NAMENODE, SECONDARY_NAMENODE, JOBTRACKER), 1,
                         get_image_id(service.cluster, opt),
                         opt.get('instance_type'), opt.get('key_name'),
                         opt.get('public_key'), opt.get('user_data_file'),
                         opt.get('availability_zone'), opt.get('user_packages'),
                         opt.get('auto_shutdown'), opt.get('env'),
                         opt.get('security_group'))
    service.launch_master(template, config_dir, opt.get('client_cidr'))

  elif command == 'launch-slaves':
    (opt, args, service) = parse_options_and_config(command, LAUNCH_OPTIONS,
                                                    ("NUM_SLAVES",))
    number_of_slaves = int(args[1])
    check_launch_options_set(service.cluster, opt)
    template = InstanceTemplate((DATANODE, TASKTRACKER), number_of_slaves,
                         get_image_id(service.cluster, opt),
                         opt.get('instance_type'), opt.get('key_name'),
                         opt.get('public_key'), opt.get('user_data_file'),
                         opt.get('availability_zone'), opt.get('user_packages'),
                         opt.get('auto_shutdown'), opt.get('env'),
                         opt.get('security_group'))
    service.launch_slaves(template)

  elif command == 'launch-cluster':
    (opt, args, service) = parse_options_and_config(command, LAUNCH_OPTIONS,
                                                    ("NUM_SLAVES",),
                                                    unbounded_args=True)
    check_launch_options_set(service.cluster, opt)
    config_dir = get_config_dir(opt)
    instance_templates = []
    if len(args) == 2:
      number_of_slaves = int(args[1])
      print_deprecation(sys.argv[0], 'launch-cluster %s 1 nn,snn,jt %s dn,tt' %
                        (service.cluster.name, number_of_slaves))
      instance_templates = [
        InstanceTemplate((NAMENODE, SECONDARY_NAMENODE, JOBTRACKER), 1,
                         get_image_id(service.cluster, opt),
                         opt.get('instance_type'), opt.get('key_name'),
                         opt.get('public_key'), opt.get('user_data_file'),
                         opt.get('availability_zone'), opt.get('user_packages'),
                         opt.get('auto_shutdown'), opt.get('env'),
                         opt.get('security_group')),
        InstanceTemplate((DATANODE, TASKTRACKER), number_of_slaves,
                         get_image_id(service.cluster, opt),
                         opt.get('instance_type'), opt.get('key_name'),
                         opt.get('public_key'), opt.get('user_data_file'),
                         opt.get('availability_zone'), opt.get('user_packages'),
                         opt.get('auto_shutdown'), opt.get('env'),
                         opt.get('security_group')),
                         ]
    elif len(args) > 2 and len(args) % 2 == 0:
      print_usage(sys.argv[0])
      sys.exit(1)
    else:
      for i in range(len(args) / 2):
        number = int(args[2 * i + 1])
        roles = args[2 * i + 2].split(",")
        instance_templates.append(
          InstanceTemplate(roles, number, get_image_id(service.cluster, opt),
                           opt.get('instance_type'), opt.get('key_name'),
                           opt.get('public_key'), opt.get('user_data_file'),
                           opt.get('availability_zone'),
                           opt.get('user_packages'),
                           opt.get('auto_shutdown'), opt.get('env'),
                           opt.get('security_group')))

    service.launch_cluster(instance_templates, config_dir,
                           opt.get('client_cidr'))

  elif command == 'login':
    (opt, args, service) = parse_options_and_config(command, SSH_OPTIONS)
    service.login(opt.get('ssh_options'))

  elif command == 'proxy':
    (opt, args, service) = parse_options_and_config(command, SSH_OPTIONS)
    service.proxy(opt.get('ssh_options'))

  elif command == 'push':
    (opt, args, service) = parse_options_and_config(command, SSH_OPTIONS,
                                                    ("FILE",))
    service.proxy(opt.get('ssh_options'), args[1])

  elif command == 'exec':
    (opt, args, service) = parse_options_and_config(command, SSH_OPTIONS,
                                                    ("CMD",), True)
    service.execute(opt.get('ssh_options'), args[1:])

  elif command == 'terminate-cluster':
    (opt, args, service) = parse_options_and_config(command, FORCE_OPTIONS)
    service.terminate_cluster(opt["force"])

  elif command == 'delete-cluster':
    (opt, args, service) = parse_options_and_config(command, BASIC_OPTIONS)
    service.delete_cluster()

  elif command == 'create-formatted-snapshot':
    (opt, args, service) = parse_options_and_config(command, SNAPSHOT_OPTIONS,
                                                    ("SIZE",))
    size = int(args[1])
    check_options_set(opt, ['availability_zone', 'key_name'])
    ami_ubuntu_intrepid_x86 = 'ami-ec48af85' # use a general AMI
    service.create_formatted_snapshot(size,
                                         opt.get('availability_zone'),
                                         ami_ubuntu_intrepid_x86,
                                         opt.get('key_name'),
                                         xstr(opt.get('ssh_options')))

  elif command == 'list-storage':
    (opt, args, service) = parse_options_and_config(command, BASIC_OPTIONS)
    service.list_storage()

  elif command == 'create-storage':
    (opt, args, service) = parse_options_and_config(command, PLACEMENT_OPTIONS,
                                                    ("ROLE", "NUM_INSTANCES",
                                                     "SPEC_FILE"))
    role = args[1]
    number_of_instances = int(args[2])
    spec_file = args[3]
    check_options_set(opt, ['availability_zone'])
    service.create_storage(role, number_of_instances,
                           opt.get('availability_zone'), spec_file)

  elif command == 'attach-storage':
    (opt, args, service) = parse_options_and_config(command, BASIC_OPTIONS,
                                                    ("ROLE",))
    service.attach_storage(args[1])

  elif command == 'delete-storage':
    (opt, args, service) = parse_options_and_config(command, FORCE_OPTIONS)
    service.delete_storage(opt["force"])

  elif command == 'update-slaves-file':
    (opt, args, service) = parse_options_and_config(command, SSH_OPTIONS)
    check_options_set(opt, ['private_key'])
    ssh_options = xstr(opt.get('ssh_options'))
    config_dir = get_config_dir(opt)
    service.update_slaves_file(config_dir, ssh_options, opt.get('private_key'))

  else:
    print "Unrecognized command '%s'" % command
    print_usage(sys.argv[0])
    sys.exit(1)
