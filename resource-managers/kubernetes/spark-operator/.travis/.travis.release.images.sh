#!/bin/bash

set -xe

OWNER="${OWNER:-radanalyticsio}"
IMAGE="${IMAGE:-spark-operator}"
[ "$TRAVIS_BRANCH" = "master" -a "$TRAVIS_PULL_REQUEST" = "false" ] && LATEST=1

main() {
  if [[ "$LATEST" = "1" ]]; then
    echo "Pushing the :latest and :latest-ubi images to docker.io and quay.io"
    loginDockerIo
    pushLatestImagesDockerIo
    loginQuayIo
    pushLatestImagesQuayIo
  elif [[ "${TRAVIS_TAG}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Pushing the '${TRAVIS_TAG}' and :latest-released images to docker.io and quay.io"
    buildReleaseImages
    loginDockerIo
    pushReleaseImages "docker.io"
    loginQuayIo
    pushReleaseImages "quay.io"
  else
    echo "Not doing the docker push, because the tag '${TRAVIS_TAG}' is not of form x.y.z"
    echo "and also it's not a build of the master branch"
  fi
}

loginDockerIo() {
  set +x
  docker login -u "$DOCKER_USERNAME" -p "$DOCKER_PASSWORD"
  set -x
}

loginQuayIo() {
  set +x
  docker login -u "$QUAY_USERNAME" -p "$QUAY_PASSWORD" quay.io
  set -x
}

pushLatestImagesDockerIo() {
  make image-publish-all
  docker logout
}

pushLatestImagesQuayIo() {
  docker tag $OWNER/$IMAGE:latest "quay.io/"$OWNER/$IMAGE:latest
  docker tag $OWNER/$IMAGE:latest-alpine "quay.io/"$OWNER/$IMAGE:latest-alpine
  docker push "quay.io/"$OWNER/$IMAGE:latest
  docker push "quay.io/"$OWNER/$IMAGE:latest-alpine
}

buildReleaseImages() {
  # build ubi image
  make build-travis image-build-all
}

pushReleaseImages() {
  if [[ $# != 1 ]] && [[ $# != 2 ]]; then
    echo "Usage: pushReleaseImages image_repo" && exit
  fi
  REPO="$1"

  docker tag $OWNER/$IMAGE:ubi $REPO/$OWNER/$IMAGE:${TRAVIS_TAG}
  docker tag $OWNER/$IMAGE:ubi $REPO/$OWNER/$IMAGE:latest-released
  docker tag $OWNER/$IMAGE:alpine $REPO/$OWNER/$IMAGE:${TRAVIS_TAG}-alpine
  docker tag $OWNER/$IMAGE:alpine $REPO/$OWNER/$IMAGE:latest-released-alpine

  # push the latest-released and ${TRAVIS_TAG} images (and also -alpine images)
  docker push $REPO/$OWNER/$IMAGE:${TRAVIS_TAG}
  docker push $REPO/$OWNER/$IMAGE:latest-released
  docker push $REPO/$OWNER/$IMAGE:${TRAVIS_TAG}-alpine
  docker push $REPO/$OWNER/$IMAGE:latest-released-alpine
  docker logout
}

main
