#!/bin/bash

ip addr show eth0 | grep "inet " | awk ' { print $2 } ' | awk ' BEGIN { FS="/" } { print $1 } '
