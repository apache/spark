#!/bin/bash
#
# This library holds functions for configuring and starting an OpenShift server

# os::start::configure_server will create and write OS master certificates, node configurations, and OpenShift configurations.
# It is recommended to run the following environment setup functions before configuring the OpenShift server:
#  - os::util::environment::setup_all_server_vars
#  - os::util::environment::use_sudo -- if your script should be using root privileges
#
# Globals:
#  - ALL_IP_ADDRESSES
#  - PUBLIC_MASTER_HOST
#  - MASTER_CONFIG_DIR
#  - SERVER_CONFIG_DIR
#  - MASTER_ADDR
#  - API_SCHEME
#  - PUBLIC_MASTER_HOST
#  - API_PORT
#  - KUBELET_SCHEME
#  - KUBELET_BIND_HOST
#  - KUBELET_PORT
#  - NODE_CONFIG_DIR
#  - KUBELET_HOST
#  - API_BIND_HOST
#  - VOLUME_DIR
#  - ETCD_DATA_DIR
#  - USE_IMAGES
#  - USE_SUDO
# Arguments:
#  1 - alternate version for the config
# Returns:
#  - export ADMIN_KUBECONFIG
#  - export CLUSTER_ADMIN_CONTEXT
function os::start::configure_server() {
	local version="${1:-}"
	local current_user
	current_user="$( id -u )"

	os::start::internal::create_master_certs     "${version}"
	os::start::internal::configure_node          "${version}"
	os::start::internal::create_bootstrap_policy "${version}"
	os::start::internal::configure_master        "${version}"

	# fix up owner after creating initial config
	${USE_SUDO:+sudo} chown -R "${current_user}" "${SERVER_CONFIG_DIR}"

	os::start::internal::patch_master_config
}
readonly -f os::start::configure_server

# os::start::internal::create_master_certs creates master certificates for the Openshift server
#
# Globals:
#  - PUBLIC_MASTER_HOST
#  - MASTER_CONFIG_DIR
#  - MASTER_ADDR
#  - API_SCHEME
#  - PUBLIC_MASTER_HOST
#  - API_PORT
# Arguments:
#  1 - alternate version for the config
function os::start::internal::create_master_certs() {
	local version="${1:-}"
	local openshift_volumes=( "${MASTER_CONFIG_DIR}" )
	local openshift_executable
	openshift_executable="$(os::start::internal::openshift_executable "${version}")"

	os::log::info "Creating certificates for the OpenShift server"
	${openshift_executable} admin create-master-certs                                   \
	                        --overwrite=false                                           \
	                        --master="${MASTER_ADDR}"                                   \
	                        --cert-dir="${MASTER_CONFIG_DIR}"                           \
	                        --hostnames="$( os::start::internal::determine_hostnames )" \
	                        --public-master="${API_SCHEME}://${PUBLIC_MASTER_HOST}:${API_PORT}"
}
readonly -f os::start::internal::create_master_certs

# os::start::internal::configure_node creates a node configuration
#
# Globals:
#  - NODE_CONFIG_DIR
#  - KUBELET_SCHEME
#  - KUBELET_BIND_HOST
#  - KUBELET_PORT
#  - KUBELET_HOST
#  - MASTER_ADDR
#  - MASTER_CONFIG_DIR
# Arguments:
#  1 - alternate version for the config
function os::start::internal::configure_node() {
	local version="${1:-}"
	local openshift_volumes=( "${MASTER_CONFIG_DIR}" "${NODE_CONFIG_DIR}" )
	local openshift_executable
	openshift_executable="$(os::start::internal::openshift_executable "${version}")"

	os::log::info "Creating node configuration for the OpenShift server"
	${openshift_executable} admin create-node-config                                          \
	                        --node-dir="${NODE_CONFIG_DIR}"                                   \
	                        --node="${KUBELET_HOST}"                                          \
	                        --hostnames="${KUBELET_HOST}"                                     \
	                        --master="${MASTER_ADDR}"                                         \
	                        --signer-cert="${MASTER_CONFIG_DIR}/ca.crt"                       \
	                        --signer-key="${MASTER_CONFIG_DIR}/ca.key"                        \
	                        --signer-serial="${MASTER_CONFIG_DIR}/ca.serial.txt"              \
	                        --certificate-authority="${MASTER_CONFIG_DIR}/ca.crt"             \
	                        --node-client-certificate-authority="${MASTER_CONFIG_DIR}/ca.crt" \
	                        --listen="${KUBELET_SCHEME}://${KUBELET_BIND_HOST}:${KUBELET_PORT}"
}
readonly -f os::start::internal::configure_node

# os::start::internal::create_bootstrap_policy creates bootstrap policy files
#
# Globals:
#  - MASTER_CONFIG_DIR
# Arguments:
#  1 - alternate version for the config
function os::start::internal::create_bootstrap_policy() {
	local version="${1:-}"
	local openshift_volumes=( "${MASTER_CONFIG_DIR}" )
	local openshift_executable
	openshift_executable="$(os::start::internal::openshift_executable "${version}")"

	os::log::info "Creating boostrap policy files for the OpenShift server"
	${openshift_executable} admin create-bootstrap-policy-file --filename="${MASTER_CONFIG_DIR}/policy.json"
}
readonly -f os::start::internal::create_bootstrap_policy

# os::start::internal::configure_master creates the configuration for the OpenShift master
#
# Globals:
#  - MASTER_CONFIG_DIR
#  - USE_IMAGES
#  - USE_SUDO
#  - API_HOST
#  - KUBELET_HOST
#  - VOLUME_DIR
#  - ETCD_DATA_DIR
#  - SERVER_CONFIG_DIR
#  - API_SCHEME
#  - API_BIND_HOST
#  - API_PORT
#  - PUBLIC_MASTER_HOST
# Arguments
#  1 - alternate version for the config
#  - MASTER_CONFIG_DIR
function os::start::internal::configure_master() {
	local version="${1:-}"
	local openshift_volumes=( "${MASTER_CONFIG_DIR}" )
	local openshift_executable
	openshift_executable="$(os::start::internal::openshift_executable "${version}")"

	os::log::info "Creating master configuration for the OpenShift server"
	${openshift_executable} start                                                   \
	                        --create-certs=false                                    \
	                        --images="${USE_IMAGES}"                                \
	                        --master="${MASTER_ADDR}"                               \
	                        --dns="tcp://${API_HOST}:53"                            \
	                        --hostname="${KUBELET_HOST}"                            \
	                        --volume-dir="${VOLUME_DIR}"                            \
	                        --etcd-dir="${ETCD_DATA_DIR}"                           \
	                        --write-config="${SERVER_CONFIG_DIR}"                   \
	                        --listen="${API_SCHEME}://${API_BIND_HOST}:${API_PORT}" \
	                        --public-master="${API_SCHEME}://${PUBLIC_MASTER_HOST}:${API_PORT}"

}
readonly -f os::start::internal::configure_master

# os::start::internal::patch_master_config patches the master configuration
#
# Globals:
#  - MASTER_CONFIG_DIR
#  - SERVER_CONFIG_DIR
#  - API_HOST
#  - ETCD_PORT
#  - ETCD_PEER_PORT
#  - USE_SUDO
#  - MAX_IMAGES_BULK_IMPORTED_PER_REPOSITORY
# Returns:
#  - export ADMIN_KUBECONFIG
#  - export CLUSTER_ADMIN_CONTEXT
function os::start::internal::patch_master_config() {
	local sudo=${USE_SUDO:+sudo}
	cp "${SERVER_CONFIG_DIR}/master/master-config.yaml" "${SERVER_CONFIG_DIR}/master/master-config.orig.yaml"
	openshift ex config patch "${SERVER_CONFIG_DIR}/master/master-config.orig.yaml" --patch="{\"etcdConfig\": {\"address\": \"${API_HOST}:${ETCD_PORT}\"}}" | \
	openshift ex config patch - --patch="{\"etcdConfig\": {\"servingInfo\": {\"bindAddress\": \"${API_HOST}:${ETCD_PORT}\"}}}" | \
	openshift ex config patch - --type json --patch="[{\"op\": \"replace\", \"path\": \"/etcdClientInfo/urls\", \"value\": [\"${API_SCHEME}://${API_HOST}:${ETCD_PORT}\"]}]" | \
	openshift ex config patch - --patch="{\"etcdConfig\": {\"peerAddress\": \"${API_HOST}:${ETCD_PEER_PORT}\"}}" | \
	openshift ex config patch - --patch="{\"etcdConfig\": {\"peerServingInfo\": {\"bindAddress\": \"${API_HOST}:${ETCD_PEER_PORT}\"}}}" | \
	openshift ex config patch - --patch="{\"imagePolicyConfig\": {\"maxImagesBulkImportedPerRepository\": ${MAX_IMAGES_BULK_IMPORTED_PER_REPOSITORY:-5}}}" > "${SERVER_CONFIG_DIR}/master/master-config.yaml"

	# Make oc use ${MASTER_CONFIG_DIR}/admin.kubeconfig, and ignore anything in the running user's $HOME dir
	export ADMIN_KUBECONFIG="${MASTER_CONFIG_DIR}/admin.kubeconfig"
	CLUSTER_ADMIN_CONTEXT=$(oc config view --config="${ADMIN_KUBECONFIG}" --flatten -o template --template='{{index . "current-context"}}'); export CLUSTER_ADMIN_CONTEXT
	${sudo} chmod -R a+rwX "${ADMIN_KUBECONFIG}"
	os::log::info "To debug: export KUBECONFIG=$ADMIN_KUBECONFIG"
}
readonly -f os::start::internal::patch_master_config

# os::start::server starts the OpenShift server, exports the PID of the OpenShift server and waits until openshift server endpoints are available
# It is advised to use this function after a successful run of 'os::start::configure_server'
#
# Globals:
#  - USE_SUDO
#  - LOG_DIR
#  - ARTIFACT_DIR
#  - VOLUME_DIR
#  - SERVER_CONFIG_DIR
#  - USE_IMAGES
#  - MASTER_ADDR
#  - MASTER_CONFIG_DIR
#  - NODE_CONFIG_DIR
#  - API_SCHEME
#  - API_HOST
#  - API_PORT
#  - KUBELET_SCHEME
#  - KUBELET_HOST
#  - KUBELET_PORT
# Arguments:
#  1 - API server version (i.e. "v1.2.0")
#  2 - Controllers version (i.e. "v1.2.0")
#  3 - Skip node start ("1" to skip node start)
# Returns:
#  - export OS_PID
#  - export ETCD_PID
#  - export API_SERVER_PID
#  - export CONTROLLERS_PID
#  - export NODE_PID
function os::start::server() {
	local api_server_version="${1:-}"
	local controllers_version="${2:-}"
	local skip_node="${3:-}"

	os::log::info "Scan of OpenShift related processes already up via ps -ef	| grep openshift : "
	ps -ef | grep openshift

	mkdir -p "${LOG_DIR}"

	if [[ -z "${api_server_version}" && -z "${controllers_version}" ]]; then
		if [[ -z "${skip_node}" ]]; then
			os::start::internal::print_server_info
			os::start::all_in_one
		else
			os::start::master
		fi
	else
		os::start::internal::print_server_info
		os::start::etcd
		os::start::api_server "${api_server_version}"
		os::start::controllers "${controllers_version}"
		if [[ -z "${skip_node}" ]]; then
			os::start::node
		fi
	fi
}
readonly -f os::start::server

# os::start::master starts the OpenShift master, exports the PID of the OpenShift master and waits until OpenShift master endpoints are available
# It is advised to use this function after a successful run of 'os::start::configure_server'
#
# Globals:
#  - USE_SUDO
#  - LOG_DIR
#  - ARTIFACT_DIR
#  - SERVER_CONFIG_DIR
#  - USE_IMAGES
#  - MASTER_ADDR
#  - MASTER_CONFIG_DIR
#  - API_SCHEME
#  - API_HOST
#  - API_PORT
# Arguments:
#  None
# Returns:
#  - export OS_PID
function os::start::master() {

	os::start::internal::print_server_info

	mkdir -p "${LOG_DIR}"

	os::log::info "Scan of OpenShift related processes already up via ps -ef	| grep openshift : "
	ps -ef | grep openshift

	os::log::info "Starting OpenShift server"
	local openshift_env=( "OPENSHIFT_PROFILE=web" "OPENSHIFT_ON_PANIC=crash" )
	$(os::start::internal::openshift_executable) start master \
		--config="${MASTER_CONFIG_DIR}/master-config.yaml" \
		--loglevel=4 --logspec='*importer=5' \
	&>"${LOG_DIR}/openshift.log" &
	export OS_PID=$!

	os::log::info "OpenShift server start at: "
	date

	os::test::junit::declare_suite_start "setup/start-master"
	os::cmd::try_until_text "oc get --raw /healthz --config='${MASTER_CONFIG_DIR}/admin.kubeconfig'" 'ok' $(( 160 * second )) 0.25
	os::cmd::try_until_text "oc get --raw /healthz/ready --config='${MASTER_CONFIG_DIR}/admin.kubeconfig'" 'ok' $(( 160 * second )) 0.25
	os::cmd::try_until_success "oc get service kubernetes --namespace default --config='${MASTER_CONFIG_DIR}/admin.kubeconfig'" $(( 160 * second )) 0.25
	os::test::junit::declare_suite_end

	os::log::info "OpenShift server health checks done at: "
	date
}
readonly -f os::start::master

# os::start::all_in_one starts the OpenShift server all in one.
# It is advised to use this function after a successful run of 'os::start::configure_server'
#
# Globals:
#  - USE_SUDO
#  - LOG_DIR
#  - ARTIFACT_DIR
#  - VOLUME_DIR
#  - SERVER_CONFIG_DIR
#  - USE_IMAGES
#  - MASTER_ADDR
#  - MASTER_CONFIG_DIR
#  - NODE_CONFIG_DIR
#  - API_SCHEME
#  - API_HOST
#  - API_PORT
#  - KUBELET_SCHEME
#  - KUBELET_HOST
#  - KUBELET_PORT
# Returns:
#  - export OS_PID
function os::start::all_in_one() {
	local use_latest_images
	if [[ -n "${USE_LATEST_IMAGES:-}" ]]; then
		use_latest_images="true"
	else
		use_latest_images="false"
	fi

	os::log::info "Starting OpenShift server"
	local openshift_env=( "OPENSHIFT_PROFILE=web" "OPENSHIFT_ON_PANIC=crash" )
	local openshift_executable
	openshift_executable="$(os::start::internal::openshift_executable)"
	${openshift_executable} start                                                       \
		                      --loglevel=4                                              \
		                      --logspec='*importer=5'                                   \
		                      --latest-images="${use_latest_images}"                    \
		                      --node-config="${NODE_CONFIG_DIR}/node-config.yaml"       \
		                      --master-config="${MASTER_CONFIG_DIR}/master-config.yaml" \
		                      &>"${LOG_DIR}/openshift.log" &
	export OS_PID=$!

	os::log::info "OpenShift server start at: "
	date

	os::test::junit::declare_suite_start "setup/start-all_in_one"
	os::cmd::try_until_text "oc get --raw /healthz --config='${MASTER_CONFIG_DIR}/admin.kubeconfig'" 'ok' $(( 80 * second )) 0.25
	os::cmd::try_until_text "oc get --raw ${KUBELET_SCHEME}://${KUBELET_HOST}:${KUBELET_PORT}/healthz --config='${MASTER_CONFIG_DIR}/admin.kubeconfig'" 'ok' $(( 2 * minute )) 0.5
	os::cmd::try_until_text "oc get --raw /healthz/ready --config='${MASTER_CONFIG_DIR}/admin.kubeconfig'" 'ok' $(( 80 * second )) 0.25
	os::cmd::try_until_success "oc get service kubernetes --namespace default --config='${MASTER_CONFIG_DIR}/admin.kubeconfig'" $(( 160 * second )) 0.25
	os::cmd::try_until_success "oc get --raw /api/v1/nodes/${KUBELET_HOST} --config='${MASTER_CONFIG_DIR}/admin.kubeconfig'" $(( 80 * second )) 0.25
	os::test::junit::declare_suite_end

	os::log::info "OpenShift server health checks done at: "
	date
}
readonly -f os::start::all_in_one

# os::start::etcd starts etcd for OpenShift
# Globals:
#  - USE_SUDO
#  - LOG_DIR
#  - MASTER_CONFIG_DIR
#  - API_SCHEME
#  - API_HOST
#  - ETCD_PORT
# Arguments:
#  None
# Returns:
#  - export ETCD_PID
function os::start::etcd() {
	os::log::info "Starting etcd"
	local openshift_env=( "OPENSHIFT_ON_PANIC=crash" )
	local openshift_executable
	openshift_executable="$(os::start::internal::openshift_executable)"
	${openshift_executable} start etcd \
		--config="${MASTER_CONFIG_DIR}/master-config.yaml" &>"${LOG_DIR}/etcd.log" &
	export ETCD_PID=$!

	os::log::info "etcd server start at: "
	date

	os::test::junit::declare_suite_start "setup/start-etcd"
	os::cmd::try_until_success "os::util::curl_etcd '/version'" $(( 10 * second ))
	os::test::junit::declare_suite_end

	os::log::info "etcd server health checks done at: "
	date
}
readonly -f os::start::etcd

# os::start::api_server starts the OpenShift API server
# Globals:
#  - USE_SUDO
#  - LOG_DIR
#  - ARTIFACT_DIR
#  - MASTER_CONFIG_DIR
#  - API_SCHEME
#  - API_HOST
#  - API_PORT
#  - KUBELET_SCHEME
#  - KUBELET_HOST
#  - KUBELET_PORT
# Arguments:
# 1 - api server version
# Returns:
#  - export OS_PID
function os::start::api_server() {
	local api_server_version=${1:-}
	local openshift_volumes=( "${MASTER_CONFIG_DIR}" )
	local openshift_env=( "OPENSHIFT_PROFILE=web" "OPENSHIFT_ON_PANIC=crash" )
	local openshift_executable
	openshift_executable="$(os::start::internal::openshift_executable "${api_server_version}")"

	${openshift_executable} start master api \
		--config="${MASTER_CONFIG_DIR}/master-config.yaml" \
	&>"${LOG_DIR}/apiserver.log" &

	export API_SERVER_PID=$!

	os::log::info "OpenShift API server start at: "
	date

	os::test::junit::declare_suite_start "setup/start-api_server"
	os::cmd::try_until_text "oc get --raw /healthz --config='${MASTER_CONFIG_DIR}/admin.kubeconfig'" 'ok' $(( 80 * second )) 0.25
	os::cmd::try_until_text "oc get --raw /healthz/ready --config='${MASTER_CONFIG_DIR}/admin.kubeconfig'" 'ok' $(( 160 * second )) 0.25
	os::test::junit::declare_suite_end

	os::log::info "OpenShift API server health checks done at: "
	date
}
readonly -f os::start::api_server

# os::start::controllers starts the OpenShift controllers
# Globals:
#  - USE_SUDO
#  - LOG_DIR
#  - MASTER_CONFIG_DIR
# Arguments:
# 1 - controllers version
# Returns:
#  - export CONTROLLERS_PID
function os::start::controllers() {
	local controllers_version=${1:-}
	local openshift_volumes=( "${MASTER_CONFIG_DIR}" )
	local openshift_env=( "OPENSHIFT_ON_PANIC=crash" )
	local openshift_executable
	openshift_executable="$(os::start::internal::openshift_executable "${controllers_version}")"

	${openshift_executable} start master controllers \
		--config="${MASTER_CONFIG_DIR}/master-config.yaml" \
	&>"${LOG_DIR}/controllers.log" &

	export CONTROLLERS_PID=$!

	os::log::info "OpenShift controllers start at: "
	date
}
readonly -f os::start::controllers


# os::start::internal::start_node starts the OpenShift node
# Globals:
#  - USE_SUDO
#  - LOG_DIR
#  - USE_LATEST_IMAGES
#  - NODE_CONFIG_DIR
#  - KUBELET_SCHEME
#  - KUBELET_HOST
#  - KUBELET_PORT
# Arguments:
#    none
# Returns:
#  - export NODE_PID
function os::start::internal::start_node() {
	local use_latest_images
	if [[ -n "${USE_LATEST_IMAGES:-}" ]]; then
		use_latest_images="true"
	else
		use_latest_images="false"
	fi

	mkdir -p "${LOG_DIR}"

	os::log::info "Starting OpenShift node"
	local openshift_env=( "OPENSHIFT_ON_PANIC=crash" )
	$(os::start::internal::openshift_executable) openshift start node \
		--config="${NODE_CONFIG_DIR}/node-config.yaml" \
		--loglevel=4 --logspec='*importer=5' \
		--latest-images="${use_latest_images}" \
	&>"${LOG_DIR}/node.log" &
	export NODE_PID=$!

	os::log::info "OpenShift node start at: "
	date

	os::test::junit::declare_suite_start "setup/start-node"
	os::cmd::try_until_text "oc get --raw ${KUBELET_SCHEME}://${KUBELET_HOST}:${KUBELET_PORT}/healthz --config='${MASTER_CONFIG_DIR}/admin.kubeconfig'" 'ok' $(( 80 * second )) 0.25
	os::test::junit::declare_suite_end

	os::log::info "OpenShift node health checks done at: "
	date
}
readonly -f os::start::internal::start_node

# os::start::internal::openshift_executable returns an openshift executable
# Vars:
#  - openshift_volumes - array of volumes to mount to openshift container (if previous version)
#  - openshift_env - array of environment variables to use when running the openshift executable
# Arguments:
#  1 - version - the version of openshift to run. If empty, execute current version
# Returns:
#  - openshift executable
function os::start::internal::openshift_executable() {
	local sudo="${USE_SUDO:+sudo}"
	local version="${1:-}"
	local openshift_executable
	if [[ -n "${version}" ]]; then
		local docker_options="--rm --privileged --net=host"
		local volumes=""
		local envvars=""

		if [[ -n "${openshift_volumes:-}" ]]; then
			for volume in "${openshift_volumes[@]}"; do
				volumes+=" -v ${volume}:${volume}"
			done
		fi

		if [[ -n "${openshift_env:-}" ]]; then
			for envvar in "${openshift_env[@]}"; do
				envvars+=" -e ${envvar}"
			done
		fi

		openshift_executable="${sudo} docker run ${docker_options} ${volumes} ${envvars} openshift/origin:${version}"
	else
		local envvars=""
		if [[ -n "${ENV:-}" ]]; then
			envvars="env "
			for envvar in "${ENV[@]}"; do
				envvars+="${envvar} "
			done
		fi

		openshift_executable="${sudo} ${envvars} $(which openshift)"
	fi

	echo "${openshift_executable}"
}
readonly -f os::start::internal::openshift_executable

# os::start::internal::determine_hostnames determines host names to add to tls cert
#
# Globals:
#  - PUBLIC_MASTER_HOST
# Returns:
#  - hostnames - list of hostnames to add to tls cert
function os::start::internal::determine_hostnames() {
	local hostnames
	hostnames="${PUBLIC_MASTER_HOST},"
	hostnames+="localhost,172.30.0.1,"
	for address in $(openshift start --print-ip); do
		hostnames+="${address},"
	done
	hostnames+="kubernetes.default.svc.cluster.local,"
	hostnames+="kubernetes.default.svc,"
	hostnames+="kubernetes.default,"
	hostnames+="kubernetes,"
	hostnames+="openshift.default.svc.cluster.local,"
	hostnames+="openshift.default.svc,"
	hostnames+="openshift.default,"
	hostnames+="openshift"

	echo "${hostnames}"
}
readonly -f os::start::internal::determine_hostnames


# os::start::internal::determine_hostnames determines host names to add to tls cert
#
# Globals:
#  - LOG_DIR
#  - SERVER_CONFIG_DIR
#  - USE_IMAGES
#  - MASTER_ADDR
function os::start::internal::print_server_info() {
	local openshift_executable
	openshift_executable="$(os::start::internal::openshift_executable)"
	os::log::info "$(${openshift_executable} version)"
	os::log::info "Server logs will be at:   ${LOG_DIR}"
	os::log::info "Config dir is:            ${SERVER_CONFIG_DIR}"
	os::log::info "Using images:             ${USE_IMAGES}"
	os::log::info "MasterIP is:              ${MASTER_ADDR}"
}

# os::start::router installs the OpenShift router and optionally creates
# the server cert as well.
#
# Globals:
#  - CREATE_ROUTER_CERT
#  - MASTER_CONFIG_DIR
#  - API_HOST
#  - ADMIN_KUBECONFIG
#  - USE_IMAGES
#  - DROP_SYN_DURING_RESTART
# Arguments:
#  None
# Returns:
#  None
function os::start::router() {
	os::log::info "Installing the router"
	oadm policy add-scc-to-user privileged --serviceaccount='router' --config="${ADMIN_KUBECONFIG}"
	# Create a TLS certificate for the router
	if [[ -n "${CREATE_ROUTER_CERT:-}" ]]; then
		os::log::info "Generating router TLS certificate"
		oadm ca create-server-cert --hostnames="*.${API_HOST}.xip.io"          \
		                           --key="${MASTER_CONFIG_DIR}/router.key"     \
		                           --cert="${MASTER_CONFIG_DIR}/router.crt"    \
		                           --signer-key="${MASTER_CONFIG_DIR}/ca.key"  \
		                           --signer-cert="${MASTER_CONFIG_DIR}/ca.crt" \
		                           --signer-serial="${MASTER_CONFIG_DIR}/ca.serial.txt"
		cat "${MASTER_CONFIG_DIR}/router.crt" \
		    "${MASTER_CONFIG_DIR}/router.key" \
			"${MASTER_CONFIG_DIR}/ca.crt" > "${MASTER_CONFIG_DIR}/router.pem"
		openshift admin router --config="${ADMIN_KUBECONFIG}" --images="${USE_IMAGES}" --service-account=router --default-cert="${MASTER_CONFIG_DIR}/router.pem"
	else
		openshift admin router --config="${ADMIN_KUBECONFIG}" --images="${USE_IMAGES}" --service-account=router
	fi

	# Set the SYN eater to make router reloads more robust
	if [[ -n "${DROP_SYN_DURING_RESTART:-}" ]]; then
		# Rewrite the DC for the router to add the environment variable into the pod definition
		os::log::info "Changing the router DC to drop SYN packets during a reload"
		oc patch dc router -p '{"spec":{"template":{"spec":{"containers":[{"name":"router","securityContext":{"privileged":true}}],"securityContext":{"runAsUser": 0}}}}}'
		oc set env dc/router -c router DROP_SYN_DURING_RESTART=true
	fi
}
readonly -f os::start::router

# os::start::registry installs the OpenShift integrated registry
#
# Globals:
#  - ADMIN_KUBECONFIG
#  - USE_IMAGES
# Arguments:
#  None
# Returns:
#  None
function os::start::registry() {
	# The --mount-host option is provided to reuse local storage.
	os::log::info "Installing the registry"
	# For testing purposes, ensure the quota objects are always up to date in the registry by
	# disabling project cache.
	openshift admin registry --config="${ADMIN_KUBECONFIG}" --images="${USE_IMAGES}" --enforce-quota -o json | \
		oc env -f - --output json "REGISTRY_MIDDLEWARE_REPOSITORY_OPENSHIFT_PROJECTCACHETTL=0" | \
		oc create -f -
}
readonly -f os::start::registry
