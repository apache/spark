/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "configuration.h"
#include "task-controller.h"

#include <errno.h>
#include <grp.h>
#include <libgen.h>
#include <limits.h>
#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#define _STRINGIFY(X) #X
#define STRINGIFY(X) _STRINGIFY(X)
#define CONF_FILENAME "taskcontroller.cfg"

void display_usage(FILE *stream) {
  fprintf(stream,
   "Usage: task-controller user good-local-dirs command command-args\n");
  fprintf(stream, " where good-local-dirs is a comma separated list of " \
          "good mapred local directories.\n");
  fprintf(stream, "Commands:\n");
  fprintf(stream, "   initialize job:       %2d jobid credentials cmd args\n",
	  INITIALIZE_JOB);
  fprintf(stream, "   launch task:          %2d jobid taskid task-script\n",
	  LAUNCH_TASK_JVM);
  fprintf(stream, "   signal task:          %2d task-pid signal\n",
	  SIGNAL_TASK);
  fprintf(stream, "   delete as user:       %2d relative-path\n",
	  DELETE_AS_USER);
  fprintf(stream, "   delete log:           %2d relative-path\n",
	  DELETE_LOG_AS_USER);
  fprintf(stream, "   run command as user:  %2d cmd args\n",
	  RUN_COMMAND_AS_USER);
}

/**
 * Return the conf dir based on the path of the executable
 */
char *infer_conf_dir(char *executable_file) {
  char *result;
  char *exec_dup = strdup(executable_file);
  char *dir = dirname(exec_dup);

  int relative_len = strlen(dir) + 1 + strlen(CONF_DIR_RELATIVE_TO_EXEC) + 1;
  char *relative_unresolved = malloc(relative_len);
  snprintf(relative_unresolved, relative_len, "%s/%s",
           dir, CONF_DIR_RELATIVE_TO_EXEC);
  result = realpath(relative_unresolved, NULL);
  // realpath will return NULL if the directory doesn't exist
  if (result == NULL) {
    fprintf(LOGFILE, "No conf directory at expected location %s\n",
            relative_unresolved);
  }
  free(exec_dup);
  free(relative_unresolved);
  return result;
}

int main(int argc, char **argv) {
  //Minimum number of arguments required to run the task-controller
  if (argc < 5) {
    display_usage(stdout);
    return INVALID_ARGUMENT_NUMBER;
  }

  LOGFILE = stdout;
  int command;
  const char * good_local_dirs = NULL;
  const char * job_id = NULL;
  const char * task_id = NULL;
  const char * cred_file = NULL;
  const char * script_file = NULL;
  const char * current_dir = NULL;
  const char * job_xml = NULL;

  int exit_code = 0;

  char * dir_to_be_deleted = NULL;

  char *executable_file = get_executable();

  char *conf_dir;

#ifndef HADOOP_CONF_DIR
  conf_dir = infer_conf_dir(argv[0]);
  if (conf_dir == NULL) {
    fprintf(LOGFILE, "Couldn't infer HADOOP_CONF_DIR. Please set in environment\n");
    return INVALID_CONFIG_FILE;
  }
#else
  conf_dir = strdup(STRINGIFY(HADOOP_CONF_DIR));
#endif

  size_t len = strlen(conf_dir) + strlen(CONF_FILENAME) + 2;
  char *orig_conf_file = malloc(len);
  snprintf(orig_conf_file, len, "%s/%s", conf_dir, CONF_FILENAME);
  char *conf_file = realpath(orig_conf_file, NULL);

  if (conf_file == NULL) {
    fprintf(LOGFILE, "Configuration file %s not found.\n", orig_conf_file);
    return INVALID_CONFIG_FILE;
  }
  free(orig_conf_file);
  free(conf_dir);
  if (check_configuration_permissions(conf_file) != 0) {
    return INVALID_CONFIG_FILE;
  }
  read_config(conf_file);
  free(conf_file);

  // look up the task tracker group in the config file
  char *tt_group = get_value(TT_GROUP_KEY);
  if (tt_group == NULL) {
    fprintf(LOGFILE, "Can't get configured value for %s.\n", TT_GROUP_KEY);
    exit(INVALID_CONFIG_FILE);
  }
  struct group *group_info = getgrnam(tt_group);
  if (group_info == NULL) {
    fprintf(LOGFILE, "Can't get group information for %s - %s.\n", tt_group,
            strerror(errno));
    exit(INVALID_CONFIG_FILE);
  }
  set_tasktracker_uid(getuid(), group_info->gr_gid);
  // if we are running from a setuid executable, make the real uid root
  setuid(0);
  // set the real and effective group id to the task tracker group
  setgid(group_info->gr_gid);

  if (check_taskcontroller_permissions(executable_file) != 0) {
    fprintf(LOGFILE, "Invalid permissions on task-controller binary.\n");
    return INVALID_TASKCONTROLLER_PERMISSIONS;
  }

  //checks done for user name
  if (argv[optind] == NULL) {
    fprintf(LOGFILE, "Invalid user name \n");
    return INVALID_USER_NAME;
  }
  int ret = set_user(argv[optind]);
  if (ret != 0) {
    return ret;
  }

  optind = optind + 1;
  good_local_dirs = argv[optind];
  if (good_local_dirs == NULL) {
    return INVALID_TT_ROOT;
  }

  optind = optind + 1;
  command = atoi(argv[optind++]);

  fprintf(LOGFILE, "main : command provided %d\n",command);
  fprintf(LOGFILE, "main : user is %s\n", user_detail->pw_name);
  fprintf(LOGFILE, "Good mapred-local-dirs are %s\n", good_local_dirs);

  switch (command) {
  case INITIALIZE_JOB:
    if (argc < 8) {
      fprintf(LOGFILE, "Too few arguments (%d vs 8) for initialize job\n",
              argc);
      return INVALID_ARGUMENT_NUMBER;
    }
    job_id = argv[optind++];
    cred_file = argv[optind++];
    job_xml = argv[optind++];
    exit_code = initialize_job(user_detail->pw_name, good_local_dirs, job_id,
                               cred_file, job_xml, argv + optind);
    break;
  case LAUNCH_TASK_JVM:
    if (argc < 8) {
      fprintf(LOGFILE, "Too few arguments (%d vs 8) for launch task\n", argc);
      return INVALID_ARGUMENT_NUMBER;
    }
    job_id = argv[optind++];
    task_id = argv[optind++];
    current_dir = argv[optind++];
    script_file = argv[optind++];
    exit_code = run_task_as_user(user_detail->pw_name, good_local_dirs, job_id,
                                 task_id, current_dir, script_file);
    break;
  case SIGNAL_TASK:
    if (argc < 6) {
      fprintf(LOGFILE, "Too few arguments (%d vs 6) for signal task\n", argc);
      return INVALID_ARGUMENT_NUMBER;
    } else {
      char* end_ptr = NULL;
      char* option = argv[optind++];
      int task_pid = strtol(option, &end_ptr, 10);
      if (option == end_ptr || *end_ptr != '\0') {
        fprintf(LOGFILE, "Illegal argument for task pid %s\n", option);
        return INVALID_ARGUMENT_NUMBER;
      }
      option = argv[optind++];
      int signal = strtol(option, &end_ptr, 10);
      if (option == end_ptr || *end_ptr != '\0') {
        fprintf(LOGFILE, "Illegal argument for signal %s\n", option);
        return INVALID_ARGUMENT_NUMBER;
      }
      exit_code = signal_user_task(user_detail->pw_name, task_pid, signal);
    }
    break;
  case DELETE_AS_USER:
    dir_to_be_deleted = argv[optind++];
    exit_code= delete_as_user(user_detail->pw_name, good_local_dirs,
                              dir_to_be_deleted);
    break;
  case DELETE_LOG_AS_USER:
    dir_to_be_deleted = argv[optind++];
    exit_code= delete_log_directory(dir_to_be_deleted, good_local_dirs);
    break;
  case RUN_COMMAND_AS_USER:
    exit_code = run_command_as_user(user_detail->pw_name, argv + optind);
    break;
  default:
    exit_code = INVALID_COMMAND_PROVIDED;
  }
  fclose(LOGFILE);
  return exit_code;
}
