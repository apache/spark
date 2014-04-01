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
#include <pwd.h>
#include <stdio.h>
#include <sys/types.h>

//command definitions
enum command {
  INITIALIZE_JOB = 0,
  LAUNCH_TASK_JVM = 1,
  SIGNAL_TASK = 2,
  DELETE_AS_USER = 3,
  DELETE_LOG_AS_USER = 4,
  RUN_COMMAND_AS_USER = 5
};

enum errorcodes {
  INVALID_ARGUMENT_NUMBER = 1,
  INVALID_USER_NAME, //2
  INVALID_COMMAND_PROVIDED, //3
  SUPER_USER_NOT_ALLOWED_TO_RUN_TASKS, //4
  INVALID_TT_ROOT, //5
  SETUID_OPER_FAILED, //6
  UNABLE_TO_EXECUTE_TASK_SCRIPT, //7
  UNABLE_TO_KILL_TASK, //8
  INVALID_TASK_PID, //9
  ERROR_RESOLVING_FILE_PATH, //10
  RELATIVE_PATH_COMPONENTS_IN_FILE_PATH, //11
  UNABLE_TO_STAT_FILE, //12
  FILE_NOT_OWNED_BY_TASKTRACKER, //13
  PREPARE_ATTEMPT_DIRECTORIES_FAILED, //14
  INITIALIZE_JOB_FAILED, //15
  PREPARE_TASK_LOGS_FAILED, //16
  INVALID_TT_LOG_DIR, //17
  OUT_OF_MEMORY, //18
  INITIALIZE_DISTCACHEFILE_FAILED, //19
  INITIALIZE_USER_FAILED, //20
  UNABLE_TO_BUILD_PATH, //21
  INVALID_TASKCONTROLLER_PERMISSIONS, //22
  PREPARE_JOB_LOGS_FAILED, //23
  INVALID_CONFIG_FILE, // 24
};

#define TT_GROUP_KEY "mapreduce.tasktracker.group"
#define CONF_DIR_RELATIVE_TO_EXEC "../../conf"

extern struct passwd *user_detail;

// the log file for error messages
extern FILE *LOGFILE;

// get the executable's filename
char* get_executable();

int check_taskcontroller_permissions(char *executable_file);

/**
 * delete a given log directory as a user
 */
int delete_log_directory(const char *log_dir, const char * good_local_dirs);

// initialize the job directory
int initialize_job(const char *user, const char * good_local_dirs, const char *jobid,
                   const char *credentials, 
                   const char *job_xml, char* const* args);

// run the task as the user
int run_task_as_user(const char * user, const char * good_local_dirs,
                     const char *jobid, const char *taskid,
                     const char *work_dir, const char *script_name);

// send a signal as the user
int signal_user_task(const char *user, int pid, int sig);

// delete a directory (or file) recursively as the user.
int delete_as_user(const char *user, const char * good_local_dirs,
                   const char *dir_to_be_deleted);

// run a command as the user
int run_command_as_user(const char *user,
                        char* const* args); 

// set the task tracker's uid and gid
void set_tasktracker_uid(uid_t user, gid_t group);

/**
 * Is the user a real user account?
 * Checks:
 *   1. Not root
 *   2. UID is above the minimum configured.
 *   3. Not in banned user list
 * Returns NULL on failure
 */
struct passwd* check_user(const char *user);

// set the user
int set_user(const char *user);

// methods to get the directories

char *get_user_directory(const char *tt_root, const char *user);

char *get_job_directory(const char * tt_root, const char *user,
                        const char *jobid);

char *get_attempt_work_directory(const char *tt_root, const char *user,
				 const char *job_dir, const char *attempt_id);

char *get_task_launcher_file(const char* work_dir);

/**
 * Get the job log directory.
 * Ensures that the result is a realpath and that it is underneath the 
 * tt log root.
 */
char* get_job_log_directory(const char* jobid);

char *get_task_log_dir(const char *log_dir, const char *job_id, 
                       const char *attempt_id);

/**
 * Ensure that the given path and all of the parent directories are created
 * with the desired permissions.
 */
int mkdirs(const char* path, mode_t perm);

/**
 * Function to initialize the user directories of a user.
 */
int initialize_user(const char *user, const char * good_local_dirs);

/**
 * Create a top level directory for the user.
 * It assumes that the parent directory is *not* writable by the user.
 * It creates directories with 02700 permissions owned by the user
 * and with the group set to the task tracker group.
 * return non-0 on failure
 */
int create_directory_for_user(const char* path);

int change_user(uid_t user, gid_t group);

/**
 * Create task attempt related directories as user.
 */
int create_attempt_directories(const char* user,
	const char * good_local_dirs, const char *job_id, const char *task_id);
