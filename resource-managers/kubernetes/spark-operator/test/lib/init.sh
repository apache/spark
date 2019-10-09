#!/bin/bash

# This script is meant to be the entrypoint for OpenShift Bash scripts to import all of the support
# libraries at once in order to make Bash script preambles as minimal as possible. This script recur-
# sively `source`s *.sh files in this directory tree. As such, no files should be `source`ed outside
# of this script to ensure that we do not attempt to overwrite read-only variables.

set -o errexit
set -o nounset
set -o pipefail

# os::util::absolute_path returns the absolute path to the directory provided
function os::util::absolute_path() {
	local relative_path="$1"
	local absolute_path

	pushd "${relative_path}" >/dev/null
	relative_path="$( pwd )"
	if [[ -h "${relative_path}" ]]; then
		absolute_path="$( readlink "${relative_path}" )"
	else
		absolute_path="${relative_path}"
	fi
	popd >/dev/null

	echo "${absolute_path}"
}
readonly -f os::util::absolute_path

# find the absolute path to the root of the Origin source tree
init_source="$( dirname "${BASH_SOURCE}" )/../.."
OS_ROOT="$( os::util::absolute_path "${init_source}" )"
export OS_ROOT
cd "${OS_ROOT}"

library_files=( $( find "${OS_ROOT}/test/lib" -type f -name '*.sh' -not -path '*/test/lib/init.sh' ) )
echo $library_files
# TODO(skuzmets): Move the contents of the following files into respective library files.
#library_files+=( "${OS_ROOT}/test/common.sh" )
#library_files+=( "${OS_ROOT}/test/util.sh" )

for library_file in "${library_files[@]}"; do
	source "${library_file}"
done

unset library_files library_file init_source

# all of our Bash scripts need to have the stacktrace
# handler installed to deal with errors
os::log::stacktrace::install

# All of our Bash scripts need to have access to the
# binaries that we build so we don't have to find
# them before every invocation.
os::util::environment::update_path_var