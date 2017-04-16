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

#include <dirent.h>
#include <fcntl.h>
#include <fts.h>
#include <errno.h>
#include <grp.h>
#include <unistd.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>

#define USER_DIR_PATTERN "%s/taskTracker/%s"

#define TT_JOB_DIR_PATTERN USER_DIR_PATTERN "/jobcache/%s"

#define ATTEMPT_DIR_PATTERN TT_JOB_DIR_PATTERN "/%s/work"

#define USER_LOG_PATTERN "%s/userlogs/%s"

#define TASK_DIR_PATTERN USER_LOG_PATTERN "/%s"

#define TASK_SCRIPT "taskjvm.sh"

#define TT_LOCAL_TASK_DIR_PATTERN "%s/taskTracker/%s/jobcache/%s/%s"

#define TT_LOG_DIR_KEY "hadoop.log.dir"

#define JOB_FILENAME "job.xml"

#define CREDENTIALS_FILENAME "jobToken"

#define MIN_USERID_KEY "min.user.id"

static const int DEFAULT_MIN_USERID = 1000;

#define BANNED_USERS_KEY "banned.users"

#define USERLOGS "userlogs"

static const char* DEFAULT_BANNED_USERS[] = {"mapred", "hdfs", "bin", 0};

//struct to store the user details
struct passwd *user_detail = NULL;

FILE* LOGFILE = NULL;

static uid_t tt_uid = -1;
static gid_t tt_gid = -1;

void set_tasktracker_uid(uid_t user, gid_t group) {
  tt_uid = user;
  tt_gid = group;
}

/**
 * get the executable filename.
 */
char* get_executable() {
  char buffer[PATH_MAX];
  snprintf(buffer, PATH_MAX, "/proc/%u/exe", getpid());
  char *filename = malloc(PATH_MAX);
  ssize_t len = readlink(buffer, filename, PATH_MAX);
  if (len == -1) {
    fprintf(stderr, "Can't get executable name from %s - %s\n", buffer,
            strerror(errno));
    exit(-1);
  } else if (len >= PATH_MAX) {
    fprintf(LOGFILE, "Executable name %.*s is longer than %d characters.\n",
            PATH_MAX, filename, PATH_MAX);
    exit(-1);
  }
  filename[len] = '\0';
  return filename;
}

/**
 * Check the permissions on taskcontroller to make sure that security is
 * promisable. For this, we need task-controller binary to
 *    * be user-owned by root
 *    * be group-owned by a configured special group.
 *    * others do not have write/execute permissions
 *    * be setuid
 */
int check_taskcontroller_permissions(char *executable_file) {

  errno = 0;
  char * resolved_path = realpath(executable_file, NULL);
  if (resolved_path == NULL) {
    fprintf(LOGFILE,
        "Error resolving the canonical name for the executable : %s!",
        strerror(errno));
    return -1;
  }

  struct stat filestat;
  errno = 0;
  if (stat(resolved_path, &filestat) != 0) {
    fprintf(LOGFILE, 
            "Could not stat the executable : %s!.\n", strerror(errno));
    return -1;
  }

  uid_t binary_euid = filestat.st_uid; // Binary's user owner
  gid_t binary_gid = filestat.st_gid; // Binary's group owner

  // Effective uid should be root
  if (binary_euid != 0) {
    fprintf(LOGFILE,
        "The task-controller binary should be user-owned by root.\n");
    return -1;
  }

  if (binary_gid != getgid()) {
    fprintf(LOGFILE, "The configured tasktracker group %d is different from"
            " the group of the executable %d\n", getgid(), binary_gid);
    return -1;
  }

  // check others do not have write/execute permissions
  if ((filestat.st_mode & S_IWOTH) == S_IWOTH ||
      (filestat.st_mode & S_IXOTH) == S_IXOTH) {
    fprintf(LOGFILE,
            "The task-controller binary should not have write or"
            " execute for others.\n");
    return -1;
  }

  // Binary should be setuid executable
  if ((filestat.st_mode & S_ISUID) == 0) {
    fprintf(LOGFILE, "The task-controller binary should be set setuid.\n");
    return -1;
  }

  return 0;
}

/**
 * Change the effective user id to limit damage.
 */
static int change_effective_user(uid_t user, gid_t group) {
  if (geteuid() == user) {
    return 0;
  }
  if (seteuid(0) != 0) {
    return -1;
  }
  if (setegid(group) != 0) {
    fprintf(LOGFILE, "Failed to set effective group id %d - %s\n", group,
            strerror(errno));
    return -1;
  }
  if (seteuid(user) != 0) {
    fprintf(LOGFILE, "Failed to set effective user id %d - %s\n", user,
            strerror(errno));
    return -1;
  }
  return 0;
}

/**
 * Change the real and effective user and group to abandon the super user
 * priviledges.
 */
int change_user(uid_t user, gid_t group) {
  if (user == getuid() && user == geteuid() && 
      group == getgid() && group == getegid()) {
    return 0;
  }

  if (seteuid(0) != 0) {
    fprintf(LOGFILE, "unable to reacquire root - %s\n", strerror(errno));
    fprintf(LOGFILE, "Real: %d:%d; Effective: %d:%d\n",
	    getuid(), getgid(), geteuid(), getegid());
    return SETUID_OPER_FAILED;
  }
  if (setgid(group) != 0) {
    fprintf(LOGFILE, "unable to set group to %d - %s\n", group, 
            strerror(errno));
    fprintf(LOGFILE, "Real: %d:%d; Effective: %d:%d\n",
	    getuid(), getgid(), geteuid(), getegid());
    return SETUID_OPER_FAILED;
  }
  if (setuid(user) != 0) {
    fprintf(LOGFILE, "unable to set user to %d - %s\n", user, strerror(errno));
    fprintf(LOGFILE, "Real: %d:%d; Effective: %d:%d\n",
	    getuid(), getgid(), geteuid(), getegid());
    return SETUID_OPER_FAILED;
  }

  return 0;
}

/**
 * Get the array of mapred local dirs from the given comma separated list of
 * paths. Memory allocated using strdup here is freed up in free_values().
 */
char ** get_mapred_local_dirs(const char * good_local_dirs) {
  return extract_values(strdup(good_local_dirs));
}

/**
 * Utility function to concatenate argB to argA using the concat_pattern.
 */
char *concatenate(char *concat_pattern, char *return_path_name, 
                  int numArgs, ...) {
  va_list ap;
  va_start(ap, numArgs);
  int strlen_args = 0;
  char *arg = NULL;
  int j;
  for (j = 0; j < numArgs; j++) {
    arg = va_arg(ap, char*);
    if (arg == NULL) {
      fprintf(LOGFILE, "One of the arguments passed for %s in null.\n",
          return_path_name);
      return NULL;
    }
    strlen_args += strlen(arg);
  }
  va_end(ap);

  char *return_path = NULL;
  int str_len = strlen(concat_pattern) + strlen_args + 1;

  return_path = (char *) malloc(str_len);
  if (return_path == NULL) {
    fprintf(LOGFILE, "Unable to allocate memory for %s.\n", return_path_name);
    return NULL;
  }
  va_start(ap, numArgs);
  vsnprintf(return_path, str_len, concat_pattern, ap);
  va_end(ap);
  return return_path;
}

/**
 * Get the job-directory path from tt_root, user name and job-id
 */
char *get_job_directory(const char * tt_root, const char *user,
                        const char *jobid) {
  return concatenate(TT_JOB_DIR_PATTERN, "job_dir_path", 3, tt_root, user,
      jobid);
}

/**
 * Get the user directory of a particular user
 */
char *get_user_directory(const char *tt_root, const char *user) {
  return concatenate(USER_DIR_PATTERN, "user_dir_path", 2, tt_root, user);
}

char *get_job_work_directory(const char *job_dir) {
  return concatenate("%s/work", "job work", 1, job_dir);
}

/**
 * Get the attempt directory for the given attempt_id
 */
char *get_attempt_work_directory(const char *tt_root, const char *user,
				 const char *job_id, const char *attempt_id) {
  return concatenate(ATTEMPT_DIR_PATTERN, "attempt_dir_path", 4,
                     tt_root, user, job_id, attempt_id);
}

char *get_task_launcher_file(const char* work_dir) {
  return concatenate("%s/%s", "task launcher", 2, work_dir, TASK_SCRIPT);
}

/**
 * Get the job log directory.
 * Ensures that the result is a realpath and that it is underneath the 
 * tt log root.
 */
char* get_job_log_directory(const char* jobid) {
  char* log_dir = get_value(TT_LOG_DIR_KEY);
  if (log_dir == NULL) {
    fprintf(LOGFILE, "Log directory %s is not configured.\n", TT_LOG_DIR_KEY);
    return NULL;
  }
  char *result = concatenate(USER_LOG_PATTERN, "job log dir", 2, log_dir, 
                             jobid);
  if (result == NULL) {
    fprintf(LOGFILE, "failed to get memory in get_job_log_directory for %s"
            " and %s\n", log_dir, jobid);
  }
  free(log_dir);
  return result;
}

/*
 * Get a user subdirectory.
 */
char *get_user_subdirectory(const char *tt_root,
                            const char *user,
                            const char *subdir) {
  char * user_dir = get_user_directory(tt_root, user);
  char * result = concatenate("%s/%s", "user subdir", 2,
                              user_dir, subdir);
  free(user_dir);
  return result;
}

/**
 * Ensure that the given path and all of the parent directories are created
 * with the desired permissions.
 */
int mkdirs(const char* path, mode_t perm) {
  char *buffer = strdup(path);
  char *token;
  int cwd = open("/", O_RDONLY);
  if (cwd == -1) {
    fprintf(LOGFILE, "Can't open / in %s - %s\n", path, strerror(errno));
    free(buffer);
    return -1;
  }
  for(token = strtok(buffer, "/"); token != NULL; token = strtok(NULL, "/")) {
    if (mkdirat(cwd, token, perm) != 0) {
      if (errno != EEXIST) {
        fprintf(LOGFILE, "Can't create directory %s in %s - %s\n", 
                token, path, strerror(errno));
        close(cwd);
        free(buffer);
        return -1;
      }
    }
    int new_dir = openat(cwd, token, O_RDONLY);
    close(cwd);
    cwd = new_dir;
    if (cwd == -1) {
      fprintf(LOGFILE, "Can't open %s in %s - %s\n", token, path, 
              strerror(errno));
      free(buffer);
      return -1;
    }
  }
  free(buffer);
  close(cwd);
  return 0;
}

static short get_current_local_dir_count(char **local_dir)
{
  char **local_dir_ptr;
  short count=0;

  for(local_dir_ptr = local_dir; *local_dir_ptr != NULL; ++local_dir_ptr) {
    ++count;
  }
  return count;
}

static char* get_nth_local_dir(char **local_dir, int nth)
{
  char **local_dir_ptr;
  short count=0;

  for(local_dir_ptr = local_dir; *local_dir_ptr != NULL; ++local_dir_ptr) {
    if(count == nth) {
      return strdup(*local_dir_ptr);
    }
    ++count;
  }
  fprintf(LOGFILE, "Invalid index %d for %d local directories\n", nth, count);
  return NULL;
}

static char*  get_random_local_dir(char **local_dir) {
  struct timeval tv;
  short nth;
  gettimeofday(&tv, NULL);
  srand ( (long) tv.tv_sec*1000000 + tv.tv_usec );
  short cnt = get_current_local_dir_count(local_dir);
  if(cnt == 0) {
    fprintf(LOGFILE, "No valid local directories\n");
    return NULL;
  }
  nth = rand() % cnt;
  return get_nth_local_dir(local_dir, nth);
}

/**
 * Function to prepare the attempt directories for the task JVM.
 * It creates the task work and log directories.
 */
int create_attempt_directories(const char* user,
    const char * good_local_dirs, const char *job_id, const char *task_id) {
  // create dirs as 0750
  const mode_t perms = S_IRWXU | S_IRGRP | S_IXGRP;
  if (job_id == NULL || task_id == NULL || user == NULL) {
    fprintf(LOGFILE, 
            "Either task_id is null or the user passed is null.\n");
    return -1;
  }
  int result = 0;

  char **local_dir = get_mapred_local_dirs(good_local_dirs);

  if (local_dir == NULL) {
    fprintf(LOGFILE, "Good mapred local directories could not be obtained.\n");
    return INVALID_TT_ROOT;
  }

  char **local_dir_ptr;
  for(local_dir_ptr = local_dir; *local_dir_ptr != NULL; ++local_dir_ptr) {
    char *task_dir = get_attempt_work_directory(*local_dir_ptr, user, job_id, 
                                                task_id);
    if (task_dir == NULL) {
      free_values(local_dir);
      return -1;
    }
    if (mkdirs(task_dir, perms) != 0) {
      // continue on to create other task directories
      free(task_dir);
    } else {
      free(task_dir);
    }
  }

  // also make the directory for the task logs
  char *job_task_name = malloc(strlen(job_id) + strlen(task_id) + 2);
  char *real_task_dir = NULL; // target of symlink
  char *real_job_dir = NULL;  // parent dir of target of symlink
  char *random_local_dir = NULL;
  char *link_task_log_dir = NULL; // symlink
  if (job_task_name == NULL) {
    fprintf(LOGFILE, "Malloc of job task name failed\n");
    result = -1;
  } else {
    sprintf(job_task_name, "%s/%s", job_id, task_id);
    link_task_log_dir = get_job_log_directory(job_task_name);
    random_local_dir = get_random_local_dir(local_dir);
    if(random_local_dir == NULL) {
      result = -1;
      goto cleanup;
    }
    real_job_dir = malloc(strlen(random_local_dir) + strlen(USERLOGS) + 
                          strlen(job_id) + 3);
    if (real_job_dir == NULL) {
      fprintf(LOGFILE, "Malloc of real job directory failed\n");
      result = -1;
      goto cleanup;
    } 
    real_task_dir = malloc(strlen(random_local_dir) + strlen(USERLOGS) + 
                           strlen(job_id) + strlen(task_id) + 4);
    if (real_task_dir == NULL) {
      fprintf(LOGFILE, "Malloc of real task directory failed\n");
      result = -1;
      goto cleanup;
    }
    sprintf(real_job_dir, USER_LOG_PATTERN, random_local_dir, job_id);
    result = create_directory_for_user(real_job_dir);
    if( result != 0) {
      result = -1;
      goto cleanup;
    }
    sprintf(real_task_dir, TASK_DIR_PATTERN,
            random_local_dir, job_id, task_id);
    result = mkdirs(real_task_dir, perms); 
    if( result != 0) {
      fprintf(LOGFILE, "Failed to create real task dir %s - %s\n",
              real_task_dir, strerror(errno));
      result = -1; 
      goto cleanup;
    }
    result = symlink(real_task_dir, link_task_log_dir);
    if( result != 0) {
      fprintf(LOGFILE, "Failed to create symlink %s to %s - %s\n",
              link_task_log_dir, real_task_dir, strerror(errno));
      result = -1;
      goto cleanup;
    }
  }

 cleanup:
  free(random_local_dir);
  free(job_task_name);
  free(link_task_log_dir);
  free(real_job_dir);
  free(real_task_dir);
  free_values(local_dir);

  return result;
}

/**
 * Load the user information for a given user name.
 */
static struct passwd* get_user_info(const char* user) {
  int string_size = sysconf(_SC_GETPW_R_SIZE_MAX);
  void* buffer = malloc(string_size + sizeof(struct passwd));
  struct passwd *result = NULL;
  if (getpwnam_r(user, buffer, buffer + sizeof(struct passwd), string_size,
		 &result) != 0) {
    free(buffer);
    fprintf(LOGFILE, "Can't get user information %s - %s\n", user,
	    strerror(errno));
    return NULL;
  }
  return result;
}

/**
 * Is the user a real user account?
 * Checks:
 *   1. Not root
 *   2. UID is above the minimum configured.
 *   3. Not in banned user list
 * Returns NULL on failure
 */
struct passwd* check_user(const char *user) {
  if (strcmp(user, "root") == 0) {
    fprintf(LOGFILE, "Running as root is not allowed\n");
    return NULL;
  }
  char *min_uid_str = get_value(MIN_USERID_KEY);
  int min_uid = DEFAULT_MIN_USERID;
  if (min_uid_str != NULL) {
    char *end_ptr = NULL;
    min_uid = strtol(min_uid_str, &end_ptr, 10);
    if (min_uid_str == end_ptr || *end_ptr != '\0') {
      fprintf(LOGFILE, "Illegal value of %s for %s in configuration\n", 
	      min_uid_str, MIN_USERID_KEY);
      free(min_uid_str);
      return NULL;
    }
    free(min_uid_str);
  }
  struct passwd *user_info = get_user_info(user);
  if (NULL == user_info) {
    fprintf(LOGFILE, "User %s not found\n", user);
    return NULL;
  }
  if (user_info->pw_uid < min_uid) {
    fprintf(LOGFILE, "Requested user %s has id %d, which is below the "
	    "minimum allowed %d\n", user, user_info->pw_uid, min_uid);
    free(user_info);
    return NULL;
  }
  char **banned_users = get_values(BANNED_USERS_KEY);
  char **banned_user = (banned_users == NULL) ? 
    (char**) DEFAULT_BANNED_USERS : banned_users;
  for(; *banned_user; ++banned_user) {
    if (strcmp(*banned_user, user) == 0) {
      free(user_info);
      fprintf(LOGFILE, "Requested user %s is banned\n", user);
      return NULL;
    }
  }
  if (banned_users != NULL) {
    free_values(banned_users);
  }
  return user_info;
}

/**
 * function used to populate and user_details structure.
 */
int set_user(const char *user) {
  // free any old user
  if (user_detail != NULL) {
    free(user_detail);
    user_detail = NULL;
  }
  user_detail = check_user(user);
  if (user_detail == NULL) {
    return -1;
  }

  if (geteuid() == user_detail->pw_uid) {
    return 0;
  }

  if (initgroups(user, user_detail->pw_gid) != 0) {
    fprintf(LOGFILE, "Error setting supplementary groups for user %s: %s\n",
        user, strerror(errno));
    return -1;
  }

  return change_effective_user(user_detail->pw_uid, user_detail->pw_gid);
}

/**
 * Change the ownership of the given file or directory to the new user.
 */
static int change_owner(const char* path, uid_t user, gid_t group) {
  if (geteuid() == user && getegid() == group) {
    return 0;
  } else {
    uid_t old_user = geteuid();
    gid_t old_group = getegid();
    if (change_effective_user(0, group) != 0) {
      return -1;
    }
    if (chown(path, user, group) != 0) {
      fprintf(LOGFILE, "Can't chown %s to %d:%d - %s\n", path, user, group,
	      strerror(errno));
      return -1;
    }
    return change_effective_user(old_user, old_group);
  }
}

/**
 * Create a top level directory for the user.
 * It assumes that the parent directory is *not* writable by the user.
 * It creates directories with 02750 permissions owned by the user
 * and with the group set to the task tracker group.
 * return non-0 on failure
 */
int create_directory_for_user(const char* path) {
  // set 2750 permissions and group sticky bit
  mode_t permissions = S_IRWXU | S_IRGRP | S_IXGRP | S_ISGID;
  uid_t user = geteuid();
  gid_t group = getegid();
  int ret = 0;
  uid_t root = 0;

  //This check is particularly required for c-based unit tests since 
  //tests run as a regular user.
  if (getuid() == root) {
    ret = change_effective_user(root, tt_gid);
  }

  if (ret == 0) {
    if (mkdir(path, permissions) == 0 || errno == EEXIST) {
      // need to reassert the group sticky bit
      if (chmod(path, permissions) != 0) {
        fprintf(LOGFILE, "Can't chmod %s to add the sticky bit - %s\n",
                path, strerror(errno));
        ret = -1;
      } else if (change_owner(path, user, tt_gid) != 0) {
        ret = -1;
      }
    } else {
      fprintf(LOGFILE, "Failed to create directory %s - %s\n", path,
              strerror(errno));
      ret = -1;
    }
  }
  if (change_effective_user(user, group) != 0) {
    ret = -1;
  }
  return ret;
}
                            
/**
 * Open a file as the tasktracker and return a file descriptor for it.
 * Returns -1 on error
 */
static int open_file_as_task_tracker(const char* filename) {
  uid_t user = geteuid();
  gid_t group = getegid();
  if (change_effective_user(tt_uid, tt_gid) != 0) {
    return -1;
  }
  int result = open(filename, O_RDONLY);
  if (result == -1) {
    fprintf(LOGFILE, "Can't open file %s as task tracker - %s\n", filename,
	    strerror(errno));
  }
  if (change_effective_user(user, group)) {
    result = -1;
  }
  return result;
}

/**
 * Copy a file from a fd to a given filename.
 * The new file must not exist and it is created with permissions perm.
 * The input stream is closed.
 * Return 0 if everything is ok.
 */
static int copy_file(int input, const char* in_filename, 
		     const char* out_filename, mode_t perm) {
  const int buffer_size = 128*1024;
  char buffer[buffer_size];
  int out_fd = open(out_filename, O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW, perm);
  if (out_fd == -1) {
    fprintf(LOGFILE, "Can't open %s for output - %s\n", out_filename, 
            strerror(errno));
    return -1;
  }
  ssize_t len = read(input, buffer, buffer_size);
  while (len > 0) {
    ssize_t pos = 0;
    while (pos < len) {
      ssize_t write_result = write(out_fd, buffer + pos, len - pos);
      if (write_result <= 0) {
	fprintf(LOGFILE, "Error writing to %s - %s\n", out_filename,
		strerror(errno));
	close(out_fd);
	return -1;
      }
      pos += write_result;
    }
    len = read(input, buffer, buffer_size);
  }
  if (len < 0) {
    fprintf(LOGFILE, "Failed to read file %s - %s\n", in_filename, 
	    strerror(errno));
    close(out_fd);
    return -1;
  }
  if (close(out_fd) != 0) {
    fprintf(LOGFILE, "Failed to close file %s - %s\n", out_filename, 
	    strerror(errno));
    return -1;
  }
  close(input);
  return 0;
}

/**
 * Function to initialize the user directories of a user.
 */
int initialize_user(const char *user, const char * good_local_dirs) {
  char **local_dir = get_mapred_local_dirs(good_local_dirs);
  if (local_dir == NULL) {
    fprintf(LOGFILE, "Good mapred local directories could ot be obtained.\n");
    return INVALID_TT_ROOT;
  }

  char *user_dir;
  char **local_dir_ptr = local_dir;
  int failed = 0;
  for(local_dir_ptr = local_dir; *local_dir_ptr != 0; ++local_dir_ptr) {
    user_dir = get_user_directory(*local_dir_ptr, user);
    if (user_dir == NULL) {
      fprintf(LOGFILE, "Couldn't get userdir directory for %s.\n", user);
      failed = 1;
      break;
    }
    if (create_directory_for_user(user_dir) != 0) {
      failed = 1;
    }
    free(user_dir);
  }
  free_values(local_dir);
  return failed ? INITIALIZE_USER_FAILED : 0;
}

/**
 * Function to prepare the job directories for the task JVM.
 */
int initialize_job(const char *user, const char * good_local_dirs,
    const char *jobid, const char* credentials, const char* job_xml,
    char* const* args) {
  if (jobid == NULL || user == NULL) {
    fprintf(LOGFILE, "Either jobid is null or the user passed is null.\n");
    return INVALID_ARGUMENT_NUMBER;
  }

  // create the user directory
  int result = initialize_user(user, good_local_dirs);
  if (result != 0) {
    return result;
  }

  // create the log directory for the job
  char *job_log_dir = get_job_log_directory(jobid);
  if (job_log_dir == NULL) {
    return -1;
  }
  result = create_directory_for_user(job_log_dir);
  free(job_log_dir);
  if (result != 0) {
    return -1;
  }

  // open up the credentials file
  int cred_file = open_file_as_task_tracker(credentials);
  if (cred_file == -1) {
    return -1;
  }

  int job_file = open_file_as_task_tracker(job_xml);
  if (job_file == -1) {
    return -1;
  }

  // give up root privs
  if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
    return -1;
  }

  // 750
  mode_t permissions = S_IRWXU | S_IRGRP | S_IXGRP;
  char **tt_roots = get_mapred_local_dirs(good_local_dirs);

  if (tt_roots == NULL) {
    return INVALID_TT_ROOT;
  }

  char **tt_root;
  char *primary_job_dir = NULL;
  for(tt_root=tt_roots; *tt_root != NULL; ++tt_root) {
    char *job_dir = get_job_directory(*tt_root, user, jobid);
    if (job_dir == NULL) {
      // try the next one
    } else if (mkdirs(job_dir, permissions) != 0) {
      free(job_dir);
    } else if (primary_job_dir == NULL) {
      primary_job_dir = job_dir;
    } else {
      free(job_dir);
    }
  }
  free_values(tt_roots);
  if (primary_job_dir == NULL) {
    fprintf(LOGFILE, "Did not create any job directories\n");
    return -1;
  }

  char *cred_file_name = concatenate("%s/%s", "cred file", 2,
				     primary_job_dir, CREDENTIALS_FILENAME);
  if (cred_file_name == NULL) {
    return -1;
  }
  if (copy_file(cred_file, credentials, cred_file_name, S_IRUSR|S_IWUSR) != 0){
    return -1;
  }
  char *job_file_name = concatenate("%s/%s", "job file", 2,
				     primary_job_dir, JOB_FILENAME);
  if (job_file_name == NULL) {
    return -1;
  }
  if (copy_file(job_file, job_xml, job_file_name,
        S_IRUSR|S_IWUSR|S_IRGRP) != 0) {
    return -1;
  }
  fclose(stdin);
  fflush(LOGFILE);
  if (LOGFILE != stdout) {
    fclose(stdout);
  }
  fclose(stderr);
  if (chdir(primary_job_dir)) {
    fprintf(LOGFILE, "Failure to chdir to job dir - %s\n",
            strerror(errno));
    return -1;
  }
  execvp(args[0], args);
  fprintf(LOGFILE, "Failure to exec job initialization process - %s\n",
	  strerror(errno));
  return -1;
}

/*
 * Function used to launch a task as the provided user. It does the following :
 * 1) Creates attempt work dir and log dir to be accessible by the child
 * 2) Copies the script file from the TT to the work directory
 * 3) Sets up the environment
 * 4) Does an execlp on the same in order to replace the current image with
 *    task image.
 */
int run_task_as_user(const char *user, const char * good_local_dirs,
                     const char *job_id, const char *task_id,
                     const char *work_dir, const char *script_name) {
  int exit_code = -1;
  char *task_script_path = NULL;
  if (create_attempt_directories(user, good_local_dirs, job_id, task_id) != 0) {
    goto cleanup;
  }
  int task_file_source = open_file_as_task_tracker(script_name);
  if (task_file_source == -1) {
    goto cleanup;
  }
  task_script_path = get_task_launcher_file(work_dir);
  if (task_script_path == NULL) {
    exit_code = OUT_OF_MEMORY;
    goto cleanup;
  }
  if (copy_file(task_file_source, script_name,task_script_path,S_IRWXU) != 0) {
    goto cleanup;
  }

  //change the user
  fcloseall();
  umask(0027);
  if (chdir(work_dir) != 0) {
    fprintf(LOGFILE, "Can't change directory to %s -%s\n", work_dir,
	    strerror(errno));
    goto cleanup;
  }
  if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
    exit_code = SETUID_OPER_FAILED;
    goto cleanup;
  }

  if (execlp(task_script_path, task_script_path, NULL) != 0) {
    fprintf(LOGFILE, "Couldn't execute the task jvm file %s - %s", 
            task_script_path, strerror(errno));
    exit_code = UNABLE_TO_EXECUTE_TASK_SCRIPT;
    goto cleanup;
  }
  exit_code = 0;

 cleanup:
  free(task_script_path);
  return exit_code;
}

/**
 * Function used to signal a task launched by the user.
 * The function sends appropriate signal to the process group
 * specified by the task_pid.
 */
int signal_user_task(const char *user, int pid, int sig) {
  if(pid <= 0) {
    return INVALID_TASK_PID;
  }

  if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
    return SETUID_OPER_FAILED;
  }

  //Don't continue if the process-group is not alive anymore.
  int has_group = 1;
  if (kill(-pid,0) < 0) {
    if (kill(pid, 0) < 0) {
      if (errno == ESRCH) {
        return INVALID_TASK_PID;
      }
      fprintf(LOGFILE, "Error signalling task %d with %d - %s\n",
	      pid, sig, strerror(errno));
      return -1;
    } else {
      has_group = 0;
    }
  }

  if (kill((has_group ? -1 : 1) * pid, sig) < 0) {
    if(errno != ESRCH) {
      fprintf(LOGFILE, 
              "Error signalling process group %d with signal %d - %s\n", 
              -pid, sig, strerror(errno));
      return UNABLE_TO_KILL_TASK;
    } else {
      return INVALID_TASK_PID;
    }
  }
  fprintf(LOGFILE, "Killing process %s%d with %d\n",
	  (has_group ? "group " :""), pid, sig);
  return 0;
}

/**
 * Delete a final directory as the task tracker user.
 */
static int rmdir_as_tasktracker(const char* path) {
  int user_uid = geteuid();
  int user_gid = getegid();
  int ret = change_effective_user(tt_uid, tt_gid);
  if (ret == 0) {
    if (rmdir(path) != 0) {
      fprintf(LOGFILE, "rmdir of %s failed - %s\n", path, strerror(errno));
      ret = -1;
    }
  }
  // always change back
  if (change_effective_user(user_uid, user_gid) != 0) {
    ret = -1;
  }
  return ret;
}

/**
 * Recursively delete the given path.
 * full_path : the path to delete
 * needs_tt_user: the top level directory must be deleted by the tt user.
 */
static int delete_path(const char *full_path, 
                       int needs_tt_user) {
  int exit_code = 0;

  if (full_path == NULL) {
    fprintf(LOGFILE, "Path is null\n");
    exit_code = UNABLE_TO_BUILD_PATH; // may be malloc failed
  } else {
    char *(paths[]) = {strdup(full_path), 0};
    if (paths[0] == NULL) {
      fprintf(LOGFILE, "Malloc failed in delete_path\n");
      return -1;
    }
    // check to make sure the directory exists
    if (access(full_path, F_OK) != 0) {
      if (errno == ENOENT) {
        free(paths[0]);
        return 0;
      }
    }
    FTS* tree = fts_open(paths, FTS_PHYSICAL | FTS_XDEV, NULL);
    FTSENT* entry = NULL;
    int ret = 0;

    if (tree == NULL) {
      fprintf(LOGFILE,
              "Cannot open file traversal structure for the path %s:%s.\n", 
              full_path, strerror(errno));
      free(paths[0]);
      return -1;
    }
    while (((entry = fts_read(tree)) != NULL) && exit_code == 0) {
      switch (entry->fts_info) {

      case FTS_DP:        // A directory being visited in post-order
        if (!needs_tt_user ||
            strcmp(entry->fts_path, full_path) != 0) {
          if (rmdir(entry->fts_accpath) != 0) {
            fprintf(LOGFILE, "Couldn't delete directory %s - %s\n", 
                    entry->fts_path, strerror(errno));
            exit_code = -1;
          }
        }
        break;

      case FTS_F:         // A regular file
      case FTS_SL:        // A symbolic link
      case FTS_SLNONE:    // A broken symbolic link
      case FTS_DEFAULT:   // Unknown type of file
        if (unlink(entry->fts_accpath) != 0) {
          fprintf(LOGFILE, "Couldn't delete file %s - %s\n", entry->fts_path,
                  strerror(errno));
          exit_code = -1;
        }
        break;

      case FTS_DNR:       // Unreadable directory
        fprintf(LOGFILE, "Unreadable directory %s. Skipping..\n", 
                entry->fts_path);
        break;

      case FTS_D:         // A directory in pre-order
        // if the directory isn't readable, chmod it
        if ((entry->fts_statp->st_mode & 0200) == 0) {
          fprintf(LOGFILE, "Unreadable directory %s, chmoding.\n", 
                  entry->fts_path);
          if (chmod(entry->fts_accpath, 0700) != 0) {
            fprintf(LOGFILE, "Error chmoding %s - %s, continuing\n", 
                    entry->fts_path, strerror(errno));
          }
        }
        break;

      case FTS_NS:        // A file with no stat(2) information
        // usually a root directory that doesn't exist
        fprintf(LOGFILE, "Directory not found %s\n", entry->fts_path);
        break;

      case FTS_DC:        // A directory that causes a cycle
      case FTS_DOT:       // A dot directory
      case FTS_NSOK:      // No stat information requested
        break;

      case FTS_ERR:       // Error return
        fprintf(LOGFILE, "Error traversing directory %s - %s\n", 
                entry->fts_path, strerror(entry->fts_errno));
        exit_code = -1;
        break;
        break;
      default:
        exit_code = -1;
        break;
      }
    }
    ret = fts_close(tree);
    if (exit_code == 0 && ret != 0) {
      fprintf(LOGFILE, "Error in fts_close while deleting %s\n", full_path);
      exit_code = -1;
    }
    if (needs_tt_user) {
      // If the delete failed, try a final rmdir as root on the top level.
      // That handles the case where the top level directory is in a directory
      // that is owned by the task tracker.
      exit_code = rmdir_as_tasktracker(full_path);
    }
    free(paths[0]);
  }
  return exit_code;
}

/**
 * Delete the given directory as the user from each of the tt_root directories
 * user: the user doing the delete
 * subdir: the subdir to delete
 */
int delete_as_user(const char *user, const char * good_local_dirs,
                   const char *subdir) {
  int ret = 0;

  char** tt_roots = get_mapred_local_dirs(good_local_dirs);
  char** ptr;
  if (tt_roots == NULL || *tt_roots == NULL) {
    fprintf(LOGFILE, "Good mapred local directories could ot be obtained.\n");
    return INVALID_TT_ROOT;
  }

  // do the delete
  for(ptr = tt_roots; *ptr != NULL; ++ptr) {
    char* full_path = get_user_subdirectory(*ptr, user, subdir);
    if (full_path == NULL) {
      return -1;
    }
    int this_ret = delete_path(full_path, strlen(subdir) == 0);
    free(full_path);
    // delete as much as we can, but remember the error
    if (this_ret != 0) {
      ret = this_ret;
    }
  }
  free_values(tt_roots);
  return ret;
}

/*
 * delete a given job log directory
 * This function takes jobid and deletes the related logs.
 */
int delete_log_directory(const char *subdir, const char * good_local_dirs) {
  char* job_log_dir = get_job_log_directory(subdir);
  
  int ret = -1;
  if (job_log_dir == NULL) return ret;

  //delete the job log directory in <hadoop.log.dir>/userlogs/jobid
  delete_path(job_log_dir, true);

  char **local_dir = get_mapred_local_dirs(good_local_dirs);

  char **local_dir_ptr;
  for(local_dir_ptr = local_dir; *local_dir_ptr != NULL; ++local_dir_ptr) {
     char *mapred_local_log_dir = concatenate(USER_LOG_PATTERN, 
				      "mapred local job log dir", 
			      	      2, *local_dir_ptr, subdir);
     if (mapred_local_log_dir != NULL) {
        //delete the job log directory in <mapred.local.dir>/userlogs/jobid
        delete_path(mapred_local_log_dir, true);
	free(mapred_local_log_dir);
     }
     else
        fprintf(LOGFILE, "Failed to delete mapred local log dir for jobid %s\n",
            subdir);
  }
  free(job_log_dir);
  free_values(local_dir);
  return 0;
}

/**
 * run command as user
 */
int run_command_as_user(const char *user, char* const* args) {
  if (user == NULL) {
    fprintf(LOGFILE, "The user passed is null.\n");
    return INVALID_ARGUMENT_NUMBER;
  }
  // give up root privs
  if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
    return -1;
  }
  execvp(args[0], args);
  fprintf(LOGFILE, "Failure to exec command - %s\n",
	  strerror(errno));
  return -1;
} 
