#!/bin/bash

# This script provides constants for the Golang binary build process

readonly OS_BUILD_ENV_GOLANG="${OS_BUILD_ENV_GOLANG:-1.7}"
readonly OS_BUILD_ENV_IMAGE="${OS_BUILD_ENV_IMAGE:-openshift/origin-release:golang-${OS_BUILD_ENV_GOLANG}}"

readonly OS_OUTPUT_SUBPATH="${OS_OUTPUT_SUBPATH:-_output/local}"
readonly OS_OUTPUT="${OS_ROOT}/${OS_OUTPUT_SUBPATH}"
readonly OS_LOCAL_RELEASEPATH="${OS_OUTPUT}/releases"
readonly OS_OUTPUT_BINPATH="${OS_OUTPUT}/bin"
readonly OS_OUTPUT_PKGDIR="${OS_OUTPUT}/pkgdir"

readonly OS_GO_PACKAGE=github.com/openshift/origin

readonly OS_SDN_COMPILE_TARGETS_LINUX=(
  pkg/sdn/plugin/sdn-cni-plugin
  vendor/github.com/containernetworking/cni/plugins/ipam/host-local
  vendor/github.com/containernetworking/cni/plugins/main/loopback
)
readonly OS_IMAGE_COMPILE_TARGETS_LINUX=(
  images/pod
  cmd/dockerregistry
  cmd/gitserver
  "${OS_SDN_COMPILE_TARGETS_LINUX[@]}"
)
readonly OS_SCRATCH_IMAGE_COMPILE_TARGETS_LINUX=(
  examples/hello-openshift
  examples/deployment
)
readonly OS_IMAGE_COMPILE_BINARIES=("${OS_SCRATCH_IMAGE_COMPILE_TARGETS_LINUX[@]##*/}" "${OS_IMAGE_COMPILE_TARGETS_LINUX[@]##*/}")

readonly OS_CROSS_COMPILE_TARGETS=(
  cmd/openshift
  cmd/oc
)
readonly OS_CROSS_COMPILE_BINARIES=("${OS_CROSS_COMPILE_TARGETS[@]##*/}")

readonly OS_TEST_TARGETS=(
  test/extended/extended.test
)

#If you update this list, be sure to get the images/origin/Dockerfile
readonly OPENSHIFT_BINARY_SYMLINKS=(
  openshift-router
  openshift-deploy
  openshift-recycle
  openshift-sti-build
  openshift-docker-build
  origin
  osc
  oadm
  osadm
  kubectl
  kubernetes
  kubelet
  kube-proxy
  kube-apiserver
  kube-controller-manager
  kube-scheduler
)
readonly OPENSHIFT_BINARY_COPY=(
  oadm
  kubelet
  kube-proxy
  kube-apiserver
  kube-controller-manager
  kube-scheduler
)
readonly OC_BINARY_COPY=(
  kubectl
)
readonly OS_BINARY_RELEASE_CLIENT_WINDOWS=(
  oc.exe
  README.md
  ./LICENSE
)
readonly OS_BINARY_RELEASE_CLIENT_MAC=(
  oc
  README.md
  ./LICENSE
)
readonly OS_BINARY_RELEASE_CLIENT_LINUX=(
  ./oc
  ./README.md
  ./LICENSE
)
readonly OS_BINARY_RELEASE_SERVER_LINUX=(
  './*'
)
readonly OS_BINARY_RELEASE_CLIENT_EXTRA=(
  ${OS_ROOT}/README.md
  ${OS_ROOT}/LICENSE
)