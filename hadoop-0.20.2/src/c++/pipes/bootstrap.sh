#!/bin/bash -xe

me=${BASH_SOURCE}

cd $(dirname $me)


aclocal -I ../utils/m4
libtoolize --automake --copy
autoconf
automake -ac --copy --add-missing
