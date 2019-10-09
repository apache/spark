#!/bin/bash

# This library holds functions that are used to clean up local
# system state after other scripts have run.

# os::cleanup::dump_etcd dumps the full contents of etcd to a file.
#
# Globals:
#  ARTIFACT_DIR
# Arguments:
#  None
# Returns:
#  None
function os::cleanup::dump_etcd() {
	os::log::info "Dumping etcd contents to ${ARTIFACT_DIR}/etcd_dump.json"
	os::util::curl_etcd "/v2/keys/?recursive=true" > "${ARTIFACT_DIR}/etcd_dump.json"
}