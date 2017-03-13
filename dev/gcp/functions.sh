#!/bin/bash
#
# functions.sh
#
# Copyright (C) 2013-2015, Levyx, Inc.
#
# NOTICE:  All information contained herein is, and remains the property of
# Levyx, Inc.  The intellectual and technical concepts contained herein are
# proprietary to Levyx, Inc. and may be covered by U.S. and Foreign Patents,
# patents in process, and are protected by trade secret or copyright law.
# Dissemination of this information or reproduction of this material is
# strictly forbidden unless prior written permission is obtained from Levyx,
# Inc.  Access to the source code contained herein is hereby forbidden to
# anyone except current Levyx, Inc. employees, managers or contractors who
# have executed Confidentiality and Non-disclosure agreements explicitly
# covering such access.
#
# Utility functions
#

err_exit() {
	echo "Error: $*"
	exit -1
}

BOLD=$(tput bold)
NORM=$(tput sgr0)
YELLOW=$(tput setaf 3)
