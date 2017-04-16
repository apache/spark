#!/bin/bash -xe

me=${BASH_SOURCE}

cd $(dirname $me)


aclocal
libtoolize --automake --copy
autoconf
automake -ac --copy --add-missing
