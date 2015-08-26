# gen-deploy-config.py
#
# Copyright (C) 2013-2015, Levyx, Inc.
#
#  NOTICE:  All information contained herein is, and remains the property of
#  Levyx, Inc.  The intellectual and technical concepts contained herein are
#  proprietary to Levyx, Inc. and may be covered by U.S. and Foreign Patents,
#  patents in process, and are protected by trade secret or copyright law.
#  Dissemination of this information or reproduction of this material is
#  strictly forbidden unless prior written permission is obtained from Levyx,
#  Inc.  Access to the source code contained herein is hereby forbidden to
#  anyone except current Levyx, Inc. employees, managers or contractors who
#  have executed Confidentiality and Non-disclosure agreements explicitly
#  covering such access.
#
# Generates YAML for gcloud deployment-manager
# Requires Python 2.7 or above
#

import argparse
import sys

parser = argparse.ArgumentParser(
    description="Script to generate YAML config",
    add_help=False
    )
parser.add_argument("-h", "--help", action="help", help="Print this help message and exit." )
parser.add_argument("-n", "--name", help="Prefix for the names of the instances.", metavar="", default="levyxspark-cluster", required=False)
parser.add_argument("-t", "--type", help="Type of machine.", metavar="", default="n1-standard-16", required=False)
parser.add_argument("-d", "--ssds", help="Number of SSDs.", metavar="", default="1", required=False)
parser.add_argument("-s", "--slaves", help="Number of slaves.", metavar="", default="0", required=False)
parser.add_argument("-r", "--startup", help="Startup script file", metavar="", default="", required=False)
args = parser.parse_args()

reqPythonVersion = (2,7)
curPythonVersion = sys.version_info

if curPythonVersion < reqPythonVersion:
    print "\nERROR: Python version must be 2.7 or above, you are running {0}.\n".format(curPythonVersion)
    sys.exit()

# ============================================================================
# Load any arguments into variables
# ============================================================================

instanceName = args.name
machineType = args.type
numSSDs = args.ssds
numSlaves = args.slaves

startupScript = "# nothing"
if ( len( args.startup ) > 0 ):
    with open ( args.startup, 'r') as startupFile:
        startupScript = startupFile.read().strip().replace('\n', " && ")
startupScript = '"{}"'.format( startupScript )
# ============================================================================
# The actual YAML
# ============================================================================
data = """
imports:
  - path: templates/cluster-instance-template.jinja
  - path: templates/cluster-setup-template.jinja
  - path: templates/cluster-network-template.jinja
  - path: templates/cluster-firewall-template.jinja
resources:
  - name: %(instanceName)s
    type: templates/cluster-setup-template.jinja
    properties:
      # package: levyxspark
      projectName: shared-cluster-0

      instanceImage: global/images/levyx-spark-dev-img
      instanceMachineType: %(machineType)s
      instanceNumSSDs: %(numSSDs)s
      instanceZone: us-central1-c
      instanceBootDiskType: pd-standard-1
      numSlaves: %(numSlaves)s
      startupScript: %(startupScript)s
"""% { "instanceName": instanceName,
        "machineType": machineType,
        "numSSDs": numSSDs,
        "numSlaves": numSlaves,
        "startupScript": startupScript }

# ============================================================================
# Write to file
# ============================================================================
with open("cluster-config.yaml", 'w') as outfile:
    outfile.write( data )
print "created cluster-config.yaml"
