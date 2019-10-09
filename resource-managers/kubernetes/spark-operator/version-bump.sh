#!/bin/bash

checkParams() {
  [[ $# -lt 1 ]] && printUsage && exit 1
  if [[ "$1" != "micro" ]] && [[ "$1" != "minor" ]] && [[ "$1" != "major" ]]; then
    printUsage
    exit 1
  fi
}

checkUntracked() {
  [[ -z $(git status -s) ]] || {
    echo "there are untracked files, commit them first"
    exit 1
  }
}

printUsage() {
  echo "usage: version-bump.sh <micro|minor|major>"
}

gitFu() {
  [[ $# -lt 2 ]] && "usage: gitFu x.y.z x'.y'.z' <micro|minor|major>" && exit 1
  old=$1
  new=$2
  mvn -U versions:set -DnewVersion=$new
  git add pom.xml
  set -x
  git commit -m "$3 version bump from $old to $new"
  set +x
}

main() {
  checkUntracked

  checkParams $@
  PARAM=$1

  CURRENT=`mvn -U org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version|grep -Ev "(^\[|Download\w+:)"|grep -v "Download"`
  VERSION=`echo $CURRENT | sed 's/-SNAPSHOT//g'`

  maj=`echo $VERSION | sed 's/^\([0-9]\+\)\..*$/\1/g'`
  min=`echo $VERSION | sed 's/^[0-9]\+\.\([0-9]\+\)\..*$/\1/g'`
  mic=`echo $VERSION | sed 's/.*\.\([0-9]\+\)$/\1/g'`

  echo "Current version: $CURRENT"
  echo "version: $VERSION"
  echo "major: $maj"
  echo "minor: $min"
  echo "micro: $mic"

  if [[ "$PARAM" = "micro" ]]; then
    echo "Updating micro version"
  elif [[ "$PARAM" = "minor" ]]; then
    echo "Updating minor version"
    ((min++))
    mic=0
  elif [[ "$PARAM" = "major" ]]; then
    echo "Updating major version"
    ((maj++))
    min=0
    mic=0
  else
    echo "Unrecognized param: $PARAM"
    printUsage && exit 1
  fi

  DESIRED="$maj.$min.$mic"
  gitFu "$CURRENT" "$DESIRED" "$PARAM"
  git tag -d "$DESIRED" || true
  git tag "$DESIRED"

  ((mic++))
  DESIRED_NEW="$maj.$min.$mic-SNAPSHOT"

  echo "Desired new version: $DESIRED_NEW"
  gitFu "$DESIRED" "$DESIRED_NEW" "$PARAM"

  echo -e "if everything is ok, you may want to continue with: \n\n git push personal master $DESIRED\n\n"
}

main $@
