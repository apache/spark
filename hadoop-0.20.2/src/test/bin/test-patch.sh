#!/usr/bin/env bash

#set -x
ulimit -n 1024

### Setup some variables.  
### JOB_NAME, SVN_REVISION, and BUILD_NUMBER are set by Hudson if it is run by patch process

###############################################################################
parseArgs() {
  case "$1" in
    HUDSON)
      ### Set HUDSON to true to indicate that this script is being run by Hudson
      HUDSON=true
      if [[ $# != 16 ]] ; then
        echo "ERROR: usage $0 HUDSON <PATCH_DIR> <SUPPORT_DIR> <PS_CMD> <WGET_CMD> <JIRACLI> <SVN_CMD> <GREP_CMD> <PATCH_CMD> <FINDBUGS_HOME> <FORREST_HOME> <ECLIPSE_HOME> <PYTHON_HOME> <WORKSPACE_BASEDIR> <TRIGGER_BUILD> <JIRA_PASSWD> "
        cleanupAndExit 0
      fi
      PATCH_DIR=$2
      SUPPORT_DIR=$3
      PS=$4
      WGET=$5
      JIRACLI=$6
      SVN=$7
      GREP=$8
      PATCH=$9
      FINDBUGS_HOME=${10}
      FORREST_HOME=${11}
      ECLIPSE_HOME=${12}
      PYTHON_HOME=${13}
      BASEDIR=${14}
      TRIGGER_BUILD_URL=${15}
      JIRA_PASSWD=${16}
      ### Retrieve the defect number
      if [ ! -e $PATCH_DIR/defectNum ] ; then
        echo "Could not determine the patch to test.  Exiting."
        cleanupAndExit 0
      fi
      defect=`cat $PATCH_DIR/defectNum`
      if [ -z "$defect" ] ; then
        echo "Could not determine the patch to test.  Exiting."
        cleanupAndExit 0
      fi
      ECLIPSE_PROPERTY="-Declipse.home=$ECLIPSE_HOME"
      PYTHON_PROPERTY="-Dpython.home=$PYTHON_HOME"
      ;;
    DEVELOPER)
      ### Set HUDSON to false to indicate that this script is being run by a developer
      HUDSON=false
      if [[ $# != 9 ]] ; then
        echo "ERROR: usage $0 DEVELOPER <PATCH_FILE> <SCRATCH_DIR> <SVN_CMD> <GREP_CMD> <PATCH_CMD> <FINDBUGS_HOME> <FORREST_HOME> <WORKSPACE_BASEDIR>"
        cleanupAndExit 0
      fi
      ### PATCH_FILE contains the location of the patchfile
      PATCH_FILE=$2 
      if [[ ! -e "$PATCH_FILE" ]] ; then
        echo "Unable to locate the patch file $PATCH_FILE"
        cleanupAndExit 0
      fi
      PATCH_DIR=$3
      ### Check if $PATCH_DIR exists. If it does not exist, create a new directory
      if [[ ! -e "$PATCH_DIR" ]] ; then
	mkdir "$PATCH_DIR"
	if [[ $? == 0 ]] ; then 
	  echo "$PATCH_DIR has been created"
	else
	  echo "Unable to create $PATCH_DIR"
	  cleanupAndExit 0
	fi
      fi
      SVN=$4
      GREP=$5
      PATCH=$6
      FINDBUGS_HOME=$7
      FORREST_HOME=$8
      BASEDIR=$9
      ### Obtain the patch filename to append it to the version number
      defect=`basename $PATCH_FILE` 
      ;;
    *)
      echo "ERROR: usage $0 HUDSON [args] | DEVELOPER [args]"
      cleanupAndExit 0
      ;;
  esac
}

###############################################################################
checkout () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Testing patch for ${defect}."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  ### When run by a developer, if the workspace contains modifications, do not continue
  status=`$SVN stat`
  if [[ $HUDSON == "false" ]] ; then
    if [[ "$status" != "" ]] ; then
      echo "ERROR: can't run in a workspace that contains the following modifications"
      echo "$status"
      cleanupAndExit 1
    fi
  else   
    cd $BASEDIR
    $SVN revert -R .
    rm -rf `$SVN status`
    $SVN update
  fi
  return $?
}

###############################################################################
setup () {
  ### Download latest patch file (ignoring .htm and .html) when run from patch process
  if [[ $HUDSON == "true" ]] ; then
    $WGET -q -O $PATCH_DIR/jira http://issues.apache.org/jira/browse/$defect
    if [[ `$GREP -c 'Patch Available' $PATCH_DIR/jira` == 0 ]] ; then
      echo "$defect is not \"Patch Available\".  Exiting."
      cleanupAndExit 0
    fi
    relativePatchURL=`$GREP -o '"/jira/secure/attachment/[0-9]*/[^"]*' $PATCH_DIR/jira | $GREP -v -e 'htm[l]*$' | sort | tail -1 | $GREP -o '/jira/secure/attachment/[0-9]*/[^"]*'`
    patchURL="http://issues.apache.org${relativePatchURL}"
    patchNum=`echo $patchURL | $GREP -o '[0-9]*/' | $GREP -o '[0-9]*'`
    echo "$defect patch is being downloaded at `date` from"
    echo "$patchURL"
    $WGET -q -O $PATCH_DIR/patch $patchURL
    VERSION=${SVN_REVISION}_${defect}_PATCH-${patchNum}
    JIRA_COMMENT="Here are the results of testing the latest attachment 
  $patchURL
  against trunk revision ${SVN_REVISION}."

    ### Copy in any supporting files needed by this process
    cp -r $SUPPORT_DIR/lib/* ./lib
    #PENDING: cp -f $SUPPORT_DIR/etc/checkstyle* ./src/test
  ### Copy the patch file to $PATCH_DIR
  else
    VERSION=PATCH-${defect}
    cp $PATCH_FILE $PATCH_DIR/patch
    if [[ $? == 0 ]] ; then
      echo "Patch file $PATCH_FILE copied to $PATCH_DIR"
    else
      echo "Could not copy $PATCH_FILE to $PATCH_DIR"
      cleanupAndExit 0
    fi
  fi
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Pre-building trunk to determine trunk number"
  echo "    of release audit, javac, and Findbugs warnings."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  ### DISABLE RELEASE AUDIT UNTIL HADOOP-4074 IS FIXED
  ### Do not call releaseaudit when run by a developer
  ### if [[ $HUDSON == "true" ]] ; then
    ### echo "$ANT_HOME/bin/ant -Dversion="${VERSION}" -DHadoopPatchProcess= releaseaudit > $PATCH_DIR/trunkReleaseAuditWarnings.txt 2>&1"
    ### $ANT_HOME/bin/ant -Dversion="${VERSION}" -DHadoopPatchProcess= releaseaudit > $PATCH_DIR/trunkReleaseAuditWarnings.txt 2>&1
  ### fi
  echo "$ANT_HOME/bin/ant -Dversion="${VERSION}" -Djavac.args="-Xlint -Xmaxwarns 1000" $ECLIPSE_PROPERTY -Dforrest.home=${FORREST_HOME} -DHadoopPatchProcess= clean tar > $PATCH_DIR/trunkJavacWarnings.txt 2>&1"
  $ANT_HOME/bin/ant -Dversion="${VERSION}" -Djavac.args="-Xlint -Xmaxwarns 1000" $ECLIPSE_PROPERTY -Dforrest.home=${FORREST_HOME} -DHadoopPatchProcess= clean tar > $PATCH_DIR/trunkJavacWarnings.txt 2>&1
  if [[ $? != 0 ]] ; then
    echo "Trunk compilation is broken?"
    cleanupAndExit 1
  fi
  echo "$ANT_HOME/bin/ant -Dversion="${VERSION}" -Dfindbugs.home=$FINDBUGS_HOME -Dforrest.home=${FORREST_HOME} -DHadoopPatchProcess= findbugs > /dev/null 2>&1"
  $ANT_HOME/bin/ant -Dversion="${VERSION}" -Dfindbugs.home=$FINDBUGS_HOME -Dforrest.home=${FORREST_HOME} -DHadoopPatchProcess= findbugs > /dev/null 2>&1
  if [[ $? != 0 ]] ; then
    echo "Trunk findbugs is broken?"
    cleanupAndExit 1
  fi
  cp $BASEDIR/build/test/findbugs/*.xml $PATCH_DIR/trunkFindbugsWarnings.xml
}

###############################################################################
### Check for @author tags in the patch
checkAuthor () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Checking there are no @author tags in the patch."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  authorTags=`$GREP -c -i '@author' $PATCH_DIR/patch`
  echo "There appear to be $authorTags @author tags in the patch."
  if [[ $authorTags != 0 ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    -1 @author.  The patch appears to contain $authorTags @author tags which the Hadoop community has agreed to not allow in code contributions."
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    +1 @author.  The patch does not contain any @author tags."
  return 0
}

###############################################################################
### Check for tests in the patch
checkTests () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Checking there are new or changed tests in the patch."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  testReferences=`$GREP -c -i '/test' $PATCH_DIR/patch`
  echo "There appear to be $testReferences test files referenced in the patch."
  if [[ $testReferences == 0 ]] ; then
    if [[ $HUDSON == "true" ]] ; then
      patchIsDoc=`$GREP -c -i 'title="documentation' $PATCH_DIR/jira`
      if [[ $patchIsDoc != 0 ]] ; then
        echo "The patch appears to be a documentation patch that doesn't require tests."
        JIRA_COMMENT="$JIRA_COMMENT

    +0 tests included.  The patch appears to be a documentation patch that doesn't require tests."
        return 0
      fi
    fi
    JIRA_COMMENT="$JIRA_COMMENT

    -1 tests included.  The patch doesn't appear to include any new or modified tests.
                        Please justify why no tests are needed for this patch."
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    +1 tests included.  The patch appears to include $testReferences new or modified tests."
  return 0
}

###############################################################################
### Attempt to apply the patch
applyPatch () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Applying patch."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  $PATCH -E -p0 < $PATCH_DIR/patch
  if [[ $? != 0 ]] ; then
    echo "PATCH APPLICATION FAILED"
    JIRA_COMMENT="$JIRA_COMMENT

    -1 patch.  The patch command could not apply the patch."
    return 1
  fi
  return 0
}

###############################################################################
### Check there are no javadoc warnings
checkJavadocWarnings () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining number of patched javadoc warnings."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "$ANT_HOME/bin/ant -Dversion="${VERSION}" -DHadoopPatchProcess= clean javadoc | tee $PATCH_DIR/patchJavadocWarnings.txt"
  $ANT_HOME/bin/ant -Dversion="${VERSION}" -DHadoopPatchProcess= clean javadoc | tee $PATCH_DIR/patchJavadocWarnings.txt
  javadocWarnings=`$GREP -c '\[javadoc\] [0-9]* warning' $PATCH_DIR/patchJavadocWarnings.txt`
  echo ""
  echo ""
  echo "There appear to be $javadocWarnings javadoc warnings generated by the patched build."
  if [[ $javadocWarnings != 0 ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    -1 javadoc.  The javadoc tool appears to have generated $javadocWarnings warning messages."
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    +1 javadoc.  The javadoc tool did not generate any warning messages."
return 0
}

###############################################################################
### Check there are no changes in the number of Javac warnings
checkJavacWarnings () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining number of patched javac warnings."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "$ANT_HOME/bin/ant -Dversion="${VERSION}" -Djavac.args="-Xlint -Xmaxwarns 1000" $ECLIPSE_PROPERTY -Dforrest.home=${FORREST_HOME} -DHadoopPatchProcess= tar > $PATCH_DIR/patchJavacWarnings.txt 2>&1"
  $ANT_HOME/bin/ant -Dversion="${VERSION}" -Djavac.args="-Xlint -Xmaxwarns 1000" $ECLIPSE_PROPERTY -Dforrest.home=${FORREST_HOME} -DHadoopPatchProcess= tar > $PATCH_DIR/patchJavacWarnings.txt 2>&1

  ### Compare trunk and patch javac warning numbers
  if [[ -f $PATCH_DIR/patchJavacWarnings.txt ]] ; then
    trunkJavacWarnings=`$GREP -o '\[javac\] [0-9]* warning' $PATCH_DIR/trunkJavacWarnings.txt | awk '{total += $2} END {print total}'`
    patchJavacWarnings=`$GREP -o '\[javac\] [0-9]* warning' $PATCH_DIR/patchJavacWarnings.txt | awk '{total += $2} END {print total}'`
    echo "There appear to be $trunkJavacWarnings javac compiler warnings before the patch and $patchJavacWarnings javac compiler warnings after applying the patch."
    if [[ $patchJavacWarnings != "" && $trunkJavacWarnings != "" ]] ; then
      if [[ $patchJavacWarnings > $trunkJavacWarnings ]] ; then
        JIRA_COMMENT="$JIRA_COMMENT

    -1 javac.  The applied patch generated $patchJavacWarnings javac compiler warnings (more than the trunk's current $trunkJavacWarnings warnings)."
        return 1
      fi
    fi
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    +1 javac.  The applied patch does not increase the total number of javac compiler warnings."
  return 0
}

###############################################################################
### Check there are no changes in the number of release audit (RAT) warnings
checkReleaseAuditWarnings () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining number of patched release audit warnings."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "$ANT_HOME/bin/ant -Dversion="${VERSION}" -Dforrest.home=${FORREST_HOME} -DHadoopPatchProcess= releaseaudit > $PATCH_DIR/patchReleaseAuditWarnings.txt 2>&1"
  $ANT_HOME/bin/ant -Dversion="${VERSION}" -Dforrest.home=${FORREST_HOME} -DHadoopPatchProcess= releaseaudit > $PATCH_DIR/patchReleaseAuditWarnings.txt 2>&1

  ### Compare trunk and patch release audit warning numbers
  if [[ -f $PATCH_DIR/patchReleaseAuditWarnings.txt ]] ; then
    trunkReleaseAuditWarnings=`$GREP -c '\!?????' $PATCH_DIR/trunkReleaseAuditWarnings.txt`
    patchReleaseAuditWarnings=`$GREP -c '\!?????' $PATCH_DIR/patchReleaseAuditWarnings.txt`
    echo ""
    echo ""
    echo "There appear to be $trunkReleaseAuditWarnings release audit warnings before the patch and $patchReleaseAuditWarnings release audit warnings after applying the patch."
    if [[ $patchReleaseAuditWarnings != "" && $trunkReleaseAuditWarnings != "" ]] ; then
      if [[ $patchReleaseAuditWarnings > $trunkReleaseAuditWarnings ]] ; then
        JIRA_COMMENT="$JIRA_COMMENT

    -1 release audit.  The applied patch generated $patchReleaseAuditWarnings release audit warnings (more than the trunk's current $trunkReleaseAuditWarnings warnings)."
        $GREP '\!?????' $PATCH_DIR/patchReleaseAuditWarnings.txt > $PATCH_DIR/patchReleaseAuditProblems.txt
        $GREP '\!?????' $PATCH_DIR/trunkReleaseAuditWarnings.txt > $PATCH_DIR/trunkReleaseAuditProblems.txt
        echo "A diff of patched release audit warnings with trunk release audit warnings." > $PATCH_DIR/releaseAuditDiffWarnings.txt
        echo "Lines that start with ????? in the release audit report indicate files that do not have an Apache license header." > $PATCH_DIR/releaseAuditDiffWarnings.txt
        echo "" > $PATCH_DIR/releaseAuditDiffWarnings.txt
        diff $PATCH_DIR/patchReleaseAuditProblems.txt $PATCH_DIR/trunkReleaseAuditProblems.txt >> $PATCH_DIR/releaseAuditDiffWarnings.txt
        JIRA_COMMENT_FOOTER="Release audit warnings: http://hudson.zones.apache.org/hudson/job/$JOB_NAME/$BUILD_NUMBER/artifact/trunk/current/releaseAuditDiffWarnings.txt
$JIRA_COMMENT_FOOTER"
        return 1
      fi
    fi
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    +1 release audit.  The applied patch does not increase the total number of release audit warnings."
  return 0
}

###############################################################################
### Check there are no changes in the number of Checkstyle warnings
checkStyle () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining number of patched checkstyle warnings."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "THIS IS NOT IMPLEMENTED YET"
  echo ""
  echo ""
  echo "$ANT_HOME/bin/ant -Dversion="${VERSION}" -DHadoopPatchProcess= checkstyle"
  $ANT_HOME/bin/ant -Dversion="${VERSION}" -DHadoopPatchProcess= checkstyle
  JIRA_COMMENT_FOOTER="Checkstyle results: http://hudson.zones.apache.org/hudson/job/$JOB_NAME/$BUILD_NUMBER/artifact/trunk/build/test/checkstyle-errors.html
$JIRA_COMMENT_FOOTER"
  ### TODO: calculate actual patchStyleErrors
#  patchStyleErrors=0
#  if [[ $patchStyleErrors != 0 ]] ; then
#    JIRA_COMMENT="$JIRA_COMMENT
#
#    -1 checkstyle.  The patch generated $patchStyleErrors code style errors."
#    return 1
#  fi
#  JIRA_COMMENT="$JIRA_COMMENT
#
#    +1 checkstyle.  The patch generated 0 code style errors."
  return 0
}

###############################################################################
### Check there are no changes in the number of Findbugs warnings
checkFindbugsWarnings () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining number of patched Findbugs warnings."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "$ANT_HOME/bin/ant -Dversion="${VERSION}" -Dfindbugs.home=$FINDBUGS_HOME -Dforrest.home=${FORREST_HOME} -DHadoopPatchProcess= findbugs"
  $ANT_HOME/bin/ant -Dversion="${VERSION}" -Dfindbugs.home=$FINDBUGS_HOME} -Dforrest.home=${FORREST_HOME} -DHadoopPatchProcess= findbugs
  if [ $? != 0 ] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    -1 findbugs.  The patch appears to cause Findbugs to fail."
    return 1
  fi
JIRA_COMMENT_FOOTER="Findbugs warnings: http://hudson.zones.apache.org/hudson/job/$JOB_NAME/$BUILD_NUMBER/artifact/trunk/build/test/findbugs/newPatchFindbugsWarnings.html
$JIRA_COMMENT_FOOTER"
  cp $BASEDIR/build/test/findbugs/*.xml $PATCH_DIR/patchFindbugsWarnings.xml
$FINDBUGS_HOME/bin/setBugDatabaseInfo -timestamp "01/01/1999" \
    $PATCH_DIR/trunkFindbugsWarnings.xml \
    $PATCH_DIR/trunkFindbugsWarnings.xml
  $FINDBUGS_HOME/bin/setBugDatabaseInfo -timestamp "01/01/2000" \
    $PATCH_DIR/patchFindbugsWarnings.xml \
    $PATCH_DIR/patchFindbugsWarnings.xml
  $FINDBUGS_HOME/bin/computeBugHistory -output $PATCH_DIR/findbugsMerge.xml \
    $PATCH_DIR/trunkFindbugsWarnings.xml \
    $PATCH_DIR/patchFindbugsWarnings.xml
  findbugsWarnings=`$FINDBUGS_HOME/bin/filterBugs -first "01/01/2000" $PATCH_DIR/findbugsMerge.xml \
    $BASEDIR/build/test/findbugs/newPatchFindbugsWarnings.xml | /usr/bin/awk '{print $1}'`
  $FINDBUGS_HOME/bin/convertXmlToText -html \
    $BASEDIR/build/test/findbugs/newPatchFindbugsWarnings.xml \
    $BASEDIR/build/test/findbugs/newPatchFindbugsWarnings.html
  cp $BASEDIR/build/test/findbugs/newPatchFindbugsWarnings.html $PATCH_DIR/newPatchFindbugsWarnings.html
  cp $BASEDIR/build/test/findbugs/newPatchFindbugsWarnings.xml $PATCH_DIR/newPatchFindbugsWarnings.xml
  if [[ $findbugsWarnings != 0 ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    -1 findbugs.  The patch appears to introduce $findbugsWarnings new Findbugs warnings."
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    +1 findbugs.  The patch does not introduce any new Findbugs warnings."
  return 0
}

###############################################################################
### Run the test-core target
runCoreTests () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Running core tests."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  
  ### Kill any rogue build processes from the last attempt
  $PS -auxwww | $GREP HadoopPatchProcess | /usr/bin/nawk '{print $2}' | /usr/bin/xargs -t -I {} /usr/bin/kill -9 {} > /dev/null

  echo "$ANT_HOME/bin/ant -Dversion="${VERSION}" -DHadoopPatchProcess= -Dtest.junit.output.format=xml -Dtest.output=yes -Dcompile.c++=yes -Dforrest.home=$FORREST_HOME create-c++-configure docs tar test-core"
  $ANT_HOME/bin/ant -Dversion="${VERSION}" -DHadoopPatchProcess= -Dtest.junit.output.format=xml -Dtest.output=yes -Dcompile.c++=yes -Dforrest.home=$FORREST_HOME create-c++-configure docs tar test-core
  if [[ $? != 0 ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    -1 core tests.  The patch failed core unit tests."
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    +1 core tests.  The patch passed core unit tests."
  return 0
}

###############################################################################
### Tests parts of contrib specific to the eclipse files
checkJarFilesDeclaredInEclipse () {
  export DECLARED_JARS=$(sed -n 's@.*kind="lib".*path="\(.*jar\)".*@\1@p' < .eclipse.templates/.classpath)
  export PRESENT_JARS=$(find build/ivy/lib/Hadoop/common/ lib/ src/test/lib/ -name '*.jar' |sort)
  # When run by Hudson, consider libs from ${SUPPORT_DIR} declared
  if [[ ${HUDSON} == "true" ]]; then
      DECLARED_JARS="${DECLARED_JARS} $(cd "${SUPPORT_DIR}"; find lib -name '*.jar')"
  fi
  DECLARED_JARS=$(sed 'y/ /\n/' <<< ${DECLARED_JARS} | sort)
  export ECLIPSE_DECLARED_SRC=$(sed -n 's@.*kind="src".*path="\(.*\)".*@\1@p' < .eclipse.templates/.classpath |sort)

  if [ "${DECLARED_JARS}" != "${PRESENT_JARS}" ]; then
    echo "
FAILED. Some jars are not declared in the Eclipse project.
  Declared jars: ${DECLARED_JARS}
  Present jars:  ${PRESENT_JARS}"
    return 1
  fi
  for dir in $ECLIPSE_DECLARED_SRC; do
    [ '!' -d $dir ] && echo "
FAILED: $dir is referenced in the Eclipse project although it doesn't exists anymore." && return 1
  done
  return 0
}

checkEclipse () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Running Eclipse classpath verification."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""

  checkJarFilesDeclaredInEclipse
  if [[ $? != 0 ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    -1 Eclipse classpath. The patch causes the Eclipse classpath to differ from the contents of the lib directories."
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    +1 Eclipse classpath. The patch retains Eclipse classpath integrity."
  return 0
}
###############################################################################
### Run the test-contrib target
runContribTests () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Running contrib tests."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""

  ### Kill any rogue build processes from the last attempt
  $PS -auxwww | $GREP HadoopPatchProcess | /usr/bin/nawk '{print $2}' | /usr/bin/xargs -t -I {} /usr/bin/kill -9 {} > /dev/null

  echo "$ANT_HOME/bin/ant -Dversion="${VERSION}" $ECLIPSE_PROPERTY $PYTHON_PROPERTY -DHadoopPatchProcess= -Dtest.junit.output.format=xml -Dtest.output=yes test-contrib"
  $ANT_HOME/bin/ant -Dversion="${VERSION}" $ECLIPSE_PROPERTY $PYTHON_PROPERTY -DHadoopPatchProcess= -Dtest.junit.output.format=xml -Dtest.output=yes test-contrib
  if [[ $? != 0 ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    -1 contrib tests.  The patch failed contrib unit tests."
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    +1 contrib tests.  The patch passed contrib unit tests."
  return 0
}

###############################################################################
### Submit a comment to the defect's Jira
submitJiraComment () {
  local result=$1
  ### Do not output the value of JIRA_COMMENT_FOOTER when run by a developer
  if [[  $HUDSON == "false" ]] ; then
    JIRA_COMMENT_FOOTER=""
  fi
  if [[ $result == 0 ]] ; then
    comment="+1 overall.  $JIRA_COMMENT

$JIRA_COMMENT_FOOTER"
  else
    comment="-1 overall.  $JIRA_COMMENT

$JIRA_COMMENT_FOOTER"
  fi
  ### Output the test result to the console
  echo "



$comment"  

  if [[ $HUDSON == "true" ]] ; then
    echo ""
    echo ""
    echo "======================================================================"
    echo "======================================================================"
    echo "    Adding comment to Jira."
    echo "======================================================================"
    echo "======================================================================"
    echo ""
    echo ""

    ### Update Jira with a comment
    export USER=hudson
    $JIRACLI -s issues.apache.org/jira login hadoopqa $JIRA_PASSWD
    $JIRACLI -s issues.apache.org/jira comment $defect "$comment"
    $JIRACLI -s issues.apache.org/jira logout
  fi
}

###############################################################################
### Cleanup files
cleanupAndExit () {
  local result=$1
  if [[ $HUDSON == "true" ]] ; then
    if [ -e "$PATCH_DIR" ] ; then
      mv $PATCH_DIR $BASEDIR
    fi
  fi
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Finished build."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  exit $result
}

###############################################################################
###############################################################################
###############################################################################

JIRA_COMMENT=""
JIRA_COMMENT_FOOTER="Console output: http://hudson.zones.apache.org/hudson/job/$JOB_NAME/$BUILD_NUMBER/console

This message is automatically generated."

### Check if arguments to the script have been specified properly or not
parseArgs $@
cd $BASEDIR

checkout
RESULT=$?
if [[ $HUDSON == "true" ]] ; then
  if [[ $RESULT != 0 ]] ; then
    ### Resubmit build.
    $WGET -q -O $PATCH_DIR/build $TRIGGER_BUILD_URL
    exit 100
  fi
fi
setup
checkAuthor
RESULT=$?

checkTests
(( RESULT = RESULT + $? ))
applyPatch
if [[ $? != 0 ]] ; then
  submitJiraComment 1
  cleanupAndExit 1
fi
checkJavadocWarnings
(( RESULT = RESULT + $? ))
checkJavacWarnings
(( RESULT = RESULT + $? ))
checkStyle
(( RESULT = RESULT + $? ))
checkFindbugsWarnings
(( RESULT = RESULT + $? ))
checkEclipse
(( RESULT = RESULT + $? ))
### Do not call these when run by a developer 
if [[ $HUDSON == "true" ]] ; then
  ### DISABLE RELEASE AUDIT UNTIL HADOOP-4074 IS FIXED
  ### checkReleaseAuditWarnings
  ### (( RESULT = RESULT + $? ))
  runCoreTests
  (( RESULT = RESULT + $? ))
  runContribTests
  (( RESULT = RESULT + $? ))
fi
JIRA_COMMENT_FOOTER="Test results: http://hudson.zones.apache.org/hudson/job/$JOB_NAME/$BUILD_NUMBER/testReport/
$JIRA_COMMENT_FOOTER"

submitJiraComment $RESULT
cleanupAndExit $RESULT


