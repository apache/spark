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
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <limits.h>

#define TEST_ROOT "/tmp/test-task-controller"
#define DONT_TOUCH_FILE "dont-touch-me"

static char* username = NULL;

/**
 * Run the command using the effective user id.
 * It can't use system, since bash seems to copy the real user id into the
 * effective id.
 */
void run(const char *cmd) {
  fflush(stdout);
  fflush(stderr);
  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: failed to fork - %s\n", strerror(errno));
  } else if (child == 0) {
    char *cmd_copy = strdup(cmd);
    char *ptr;
    int words = 1;
    for(ptr = strchr(cmd_copy, ' ');  ptr; ptr = strchr(ptr+1, ' ')) {
      words += 1;
    }
    char **argv = malloc(sizeof(char *) * (words + 1));
    ptr = strtok(cmd_copy, " ");
    int i = 0;
    argv[i++] = ptr;
    while (ptr != NULL) {
      ptr = strtok(NULL, " ");
      argv[i++] = ptr;
    }
    if (execvp(argv[0], argv) != 0) {
      printf("FAIL: exec failed in child %s - %s\n", cmd, strerror(errno));
      exit(42);
    }
  } else {
    int status = 0;
    if (waitpid(child, &status, 0) <= 0) {
      printf("FAIL: failed waiting for child process %s pid %d - %s\n", 
	     cmd, child, strerror(errno));
      exit(1);
    }
    if (!WIFEXITED(status)) {
      printf("FAIL: process %s pid %d did not exit\n", cmd, child);
      exit(1);
    }
    if (WEXITSTATUS(status) != 0) {
      printf("FAIL: process %s pid %d exited with error status %d\n", cmd, 
	     child, WEXITSTATUS(status));
      exit(1);
    }
  }
}

int write_config_file(char *file_name) {
  FILE *file;
  file = fopen(file_name, "w");
  if (file == NULL) {
    printf("Failed to open %s.\n", file_name);
    return EXIT_FAILURE;
  }
  fprintf(file, "mapred.local.dir=" TEST_ROOT "/local-1");
  int i;
  for(i=2; i < 5; ++i) {
    fprintf(file, "," TEST_ROOT "/local-%d", i);
  }
  fprintf(file, "\n");
  fprintf(file, "hadoop.log.dir=" TEST_ROOT "/logs");
  fclose(file);
  return 0;
}

void create_tt_roots() {
  char** tt_roots = get_values("mapred.local.dir");
  char** tt_root;
  for(tt_root=tt_roots; *tt_root != NULL; ++tt_root) {
    if (mkdir(*tt_root, 0755) != 0) {
      printf("FAIL: Can't create directory %s - %s\n", *tt_root,
	     strerror(errno));
      exit(1);
    }
    char buffer[100000];
    sprintf(buffer, "%s/taskTracker", *tt_root);
    if (mkdir(buffer, 0755) != 0) {
      printf("FAIL: Can't create directory %s - %s\n", buffer,
	     strerror(errno));
      exit(1);
    }
  }
  free_values(tt_roots);
}

void test_get_user_directory() {
  char *user_dir = get_user_directory("/tmp", "user");
  char *expected = "/tmp/taskTracker/user";
  if (strcmp(user_dir, expected) != 0) {
    printf("test_get_user_directory expected %s got %s\n", user_dir, expected);
    exit(1);
  }
  free(user_dir);
}

void test_get_job_directory() {
  char *expected = "/tmp/taskTracker/user/jobcache/job_200906101234_0001";
  char *job_dir = (char *) get_job_directory("/tmp", "user",
      "job_200906101234_0001");
  if (strcmp(job_dir, expected) != 0) {
    exit(1);
  }
  free(job_dir);
}

void test_get_attempt_directory() {
  char *attempt_dir = get_attempt_work_directory("/tmp", "owen", "job_1",
						 "attempt_1");
  char *expected = "/tmp/taskTracker/owen/jobcache/job_1/attempt_1/work";
  if (strcmp(attempt_dir, expected) != 0) {
    printf("Fail get_attempt_work_directory got %s expected %s\n",
	   attempt_dir, expected);
  }
  free(attempt_dir);
}

void test_get_task_launcher_file() {
  char *expected_file = ("/tmp/taskTracker/user/jobcache/job_200906101234_0001"
			 "/taskjvm.sh");
  char *job_dir = get_job_directory("/tmp", "user",
                                    "job_200906101234_0001");
  char *task_file =  get_task_launcher_file(job_dir);
  if (strcmp(task_file, expected_file) != 0) {
    printf("failure to match expected task file %s vs %s\n", task_file,
           expected_file);
    exit(1);
  }
  free(job_dir);
  free(task_file);
}

void test_get_job_log_dir() {
  char *expected = TEST_ROOT "/logs/userlogs/job_200906101234_0001";
  char *logdir = get_job_log_directory("job_200906101234_0001");
  if (strcmp(logdir, expected) != 0) {
    printf("Fail get_job_log_dir got %s expected %s\n", logdir, expected);
    exit(1);
  }
  free(logdir);
}

void test_get_task_log_dir() {
  char *logdir = get_job_log_directory("job_5/task_4");
  char *expected = TEST_ROOT "/logs/userlogs/job_5/task_4";
  if (strcmp(logdir, expected) != 0) {
    printf("FAIL: get_task_log_dir expected %s got %s\n", logdir, expected);
  }
  free(logdir);
}

void create_userlogs_dir() {
  char** tt_roots = get_values("mapred.local.dir");
  char** tt_root;
  for(tt_root=tt_roots; *tt_root != NULL; ++tt_root) {
    char buffer[100000];
    sprintf(buffer, "%s/userlogs", *tt_root);
    if (mkdir(buffer, 0755) != 0) {
      printf("FAIL: Can't create directory %s - %s\n", buffer,
             strerror(errno));
      exit(1);
    }
  }
  free_values(tt_roots);
}

void test_create_log_directory() {
  printf("\nTesting test_create_log_directory\n");
  create_userlogs_dir();
  char *job_log_dir = get_job_log_directory("job_7");
  if (job_log_dir == NULL) {
    exit(1);
  }
  if (create_directory_for_user(job_log_dir) != 0) {
    exit(1);
  }
  free(job_log_dir);
  char* good_local_dirs = get_value("mapred.local.dir");
  if (good_local_dirs == NULL) {
    fprintf(LOGFILE, "Mapred local directories could not be obtained.\n");
    exit(1);
  }
  create_attempt_directories(username, good_local_dirs, "job_7", "task_1");

  //check if symlink got created
  struct stat file;
  int status;
  char actualpath [PATH_MAX+1];
  char *res;
  char *filepath = TEST_ROOT "/logs/userlogs/job_7/task_1";

  status = lstat(filepath, &file);
  if (!S_ISLNK(file.st_mode)) {
    fprintf(LOGFILE, "Symlink creation failed\n");
    exit(1);
  }

  //Check if symlink path exists
  res = realpath(filepath, actualpath);
  if(!res) {
    fprintf(LOGFILE, "Failed to get target for the symlink\n");
    exit(1);
  }

  char local_job_dir[PATH_MAX+1];
  int i;
  bool found = false;
  for(i=1; i<5; i++) {
     sprintf(local_job_dir, TEST_ROOT "/local-%d/userlogs/job_7/task_1", i);
     if (strcmp(local_job_dir, actualpath) == 0) {
       found = true;
       break;
     }
  }
  
  if(!found) {
    printf("FAIL: symlink path and target path mismatch\n");
    exit(1);
  }

  free(good_local_dirs);
}

void test_check_user() {
  printf("\nTesting test_check_user\n");
  struct passwd *user = check_user(username);
  if (user == NULL) {
    printf("FAIL: failed check for user %s\n", username);
    exit(1);
  }
  free(user);
  if (check_user("lp") != NULL) {
    printf("FAIL: failed check for system user lp\n");
    exit(1);
  }
  if (check_user("root") != NULL) {
    printf("FAIL: failed check for system user root\n");
    exit(1);
  }
  if (check_user("mapred") != NULL) {
    printf("FAIL: failed check for hadoop user mapred\n");
    exit(1);
  }
}

void test_check_configuration_permissions() {
  printf("\nTesting check_configuration_permissions\n");
  if (check_configuration_permissions("/etc/passwd") != 0) {
    printf("FAIL: failed permission check on /etc/passwd\n");
    exit(1);
  }
  if (check_configuration_permissions(TEST_ROOT) == 0) {
    printf("FAIL: failed permission check on %s\n", TEST_ROOT);
    exit(1);
  }
}

void test_delete_task() {
  char* local_dirs = get_value("mapred.local.dir");
  if (initialize_user(username, local_dirs)) {
    printf("FAIL: failed to initialize user %s\n", username);
    exit(1);
  }
  char* job_dir = get_job_directory(TEST_ROOT "/local-2", username, "job_1");
  char* dont_touch = get_job_directory(TEST_ROOT "/local-2", username, 
                                       DONT_TOUCH_FILE);
  char* task_dir = get_attempt_work_directory(TEST_ROOT "/local-2", 
					      username, "job_1", "task_1");
  char buffer[100000];
  sprintf(buffer, "mkdir -p %s/who/let/the/dogs/out/who/who", task_dir);
  run(buffer);
  sprintf(buffer, "touch %s", dont_touch);
  run(buffer);

  // soft link to the canary file from the task directory
  sprintf(buffer, "ln -s %s %s/who/softlink", dont_touch, task_dir);
  run(buffer);
  // hard link to the canary file from the task directory
  sprintf(buffer, "ln %s %s/who/hardlink", dont_touch, task_dir);
  run(buffer);
  // create a dot file in the task directory
  sprintf(buffer, "touch %s/who/let/.dotfile", task_dir);
  run(buffer);
  // create a no permission file
  sprintf(buffer, "touch %s/who/let/protect", task_dir);
  run(buffer);
  sprintf(buffer, "chmod 000 %s/who/let/protect", task_dir);
  run(buffer);
  // create a no permission directory
  sprintf(buffer, "chmod 000 %s/who/let", task_dir);
  run(buffer);

  // delete task directory
  int ret = delete_as_user(username, local_dirs, "jobcache/job_1/task_1");
  if (ret != 0) {
    printf("FAIL: return code from delete_as_user is %d\n", ret);
    exit(1);
  }

  // check to make sure the task directory is gone
  if (access(task_dir, R_OK) == 0) {
    printf("FAIL: failed to delete the directory - %s\n", task_dir);
    exit(1);
  }
  // check to make sure the job directory is not gone
  if (access(job_dir, R_OK) != 0) {
    printf("FAIL: accidently deleted the directory - %s\n", job_dir);
    exit(1);
  }
  // but that the canary is not gone
  if (access(dont_touch, R_OK) != 0) {
    printf("FAIL: accidently deleted file %s\n", dont_touch);
    exit(1);
  }
  sprintf(buffer, "chmod -R 700 %s", job_dir);
  run(buffer);
  sprintf(buffer, "rm -fr %s", job_dir);
  run(buffer);
  free(job_dir);
  free(task_dir);
  free(dont_touch);
  free(local_dirs);
}

void test_delete_job() {
  char* local_dirs = get_value("mapred.local.dir");
  char* job_dir = get_job_directory(TEST_ROOT "/local-2", username, "job_2");
  char* dont_touch = get_job_directory(TEST_ROOT "/local-2", username, 
                                       DONT_TOUCH_FILE);
  char* task_dir = get_attempt_work_directory(TEST_ROOT "/local-2", 
					      username, "job_2", "task_1");
  char buffer[100000];
  sprintf(buffer, "mkdir -p %s/who/let/the/dogs/out/who/who", task_dir);
  run(buffer);
  sprintf(buffer, "touch %s", dont_touch);
  run(buffer);

  // soft link to the canary file from the task directory
  sprintf(buffer, "ln -s %s %s/who/softlink", dont_touch, task_dir);
  run(buffer);
  // hard link to the canary file from the task directory
  sprintf(buffer, "ln %s %s/who/hardlink", dont_touch, task_dir);
  run(buffer);
  // create a dot file in the task directory
  sprintf(buffer, "touch %s/who/let/.dotfile", task_dir);
  run(buffer);
  // create a no permission file
  sprintf(buffer, "touch %s/who/let/protect", task_dir);
  run(buffer);
  sprintf(buffer, "chmod 000 %s/who/let/protect", task_dir);
  run(buffer);
  // create a no permission directory
  sprintf(buffer, "chmod 000 %s/who/let", task_dir);
  run(buffer);

  // delete task directory
  int ret = delete_as_user(username, local_dirs, "jobcache/job_2");
  if (ret != 0) {
    printf("FAIL: return code from delete_as_user is %d\n", ret);
    exit(1);
  }

  // check to make sure the task directory is gone
  if (access(task_dir, R_OK) == 0) {
    printf("FAIL: failed to delete the directory - %s\n", task_dir);
    exit(1);
  }
  // check to make sure the job directory is gone
  if (access(job_dir, R_OK) == 0) {
    printf("FAIL: didn't delete the directory - %s\n", job_dir);
    exit(1);
  }
  // but that the canary is not gone
  if (access(dont_touch, R_OK) != 0) {
    printf("FAIL: accidently deleted file %s\n", dont_touch);
    exit(1);
  }
  free(job_dir);
  free(task_dir);
  free(dont_touch);
  free(local_dirs);
}


void test_delete_user() {
  printf("\nTesting delete_user\n");
  char* local_dirs = get_value("mapred.local.dir");
  char* job_dir = get_job_directory(TEST_ROOT "/local-1", username, "job_3");
  if (mkdirs(job_dir, 0700) != 0) {
    exit(1);
  }
  char buffer[100000];
  sprintf(buffer, "%s/local-1/taskTracker/%s", TEST_ROOT, username);
  if (access(buffer, R_OK) != 0) {
    printf("FAIL: directory missing before test\n");
    exit(1);
  }
  if (delete_as_user(username, local_dirs, "") != 0) {
    exit(1);
  }
  if (access(buffer, R_OK) == 0) {
    printf("FAIL: directory not deleted\n");
    exit(1);
  }
  if (access(TEST_ROOT "/local-1", R_OK) != 0) {
    printf("FAIL: local-1 directory does not exist\n");
    exit(1);
  }
  free(job_dir);
  free(local_dirs);
}

void test_delete_log_directory() {
  printf("\nTesting delete_log_directory\n");
  char* local_dirs = get_value("mapred.local.dir");
  char *job_log_dir = get_job_log_directory("job_1");
  if (job_log_dir == NULL) {
    exit(1);
  }
  if (create_directory_for_user(job_log_dir) != 0) {
    exit(1);
  }
  free(job_log_dir);
  char *task_log_dir = get_job_log_directory("job_1/task_2");
  if (task_log_dir == NULL) {
    exit(1);
  }
  if (mkdirs(task_log_dir, 0700) != 0) {
    exit(1);
  }
  if (access(TEST_ROOT "/logs/userlogs/job_1/task_2", R_OK) != 0) {
    printf("FAIL: can't access task directory - %s\n", strerror(errno));
    exit(1);
  }
  if (delete_log_directory("job_1/task_2", local_dirs) != 0) {
    printf("FAIL: can't delete task directory\n");
    exit(1);
  }
  if (access(TEST_ROOT "/logs/userlogs/job_1/task_2", R_OK) == 0) {
    printf("FAIL: task directory not deleted\n");
    exit(1);
  }
  if (access(TEST_ROOT "/logs/userlogs/job_1", R_OK) != 0) {
    printf("FAIL: job directory not deleted - %s\n", strerror(errno));
    exit(1);
  }
  if (delete_log_directory("job_1", local_dirs) != 0) {
    printf("FAIL: can't delete task directory\n");
    exit(1);
  }
  if (access(TEST_ROOT "/logs/userlogs/job_1", R_OK) == 0) {
    printf("FAIL: job directory not deleted\n");
    exit(1);
  }
  if (delete_log_directory("job_7", local_dirs) != 0) {
    printf("FAIL: can't delete job directory\n");
    exit(1);
  }
  if (access(TEST_ROOT "/logs/userlogs/job_7", R_OK) == 0) {
    printf("FAIL: job log directory not deleted\n");
    exit(1);
  }
  char local_job_dir[PATH_MAX+1];
  int i;
  for(i=1; i<5; i++) {
     sprintf(local_job_dir, TEST_ROOT "/local-%d/userlogs/job_7", i);
     if (access(local_job_dir, R_OK) == 0) {
       printf("FAIL: job log directory in mapred local not deleted\n");
       exit(1);
     }
  }
  free(task_log_dir);
  free(local_dirs);
}

void run_test_in_child(const char* test_name, void (*func)()) {
  printf("\nRunning test %s in child process\n", test_name);
  fflush(stdout);
  fflush(stderr);
  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: fork failed\n");
    exit(1);
  } else if (child == 0) {
    func();
    exit(0);
  } else {
    int status = 0;
    if (waitpid(child, &status, 0) == -1) {
      printf("FAIL: waitpid %d failed - %s\n", child, strerror(errno));
      exit(1);
    }
    if (!WIFEXITED(status)) {
      printf("FAIL: child %d didn't exit - %d\n", child, status);
      exit(1);
    }
    if (WEXITSTATUS(status) != 0) {
      printf("FAIL: child %d exited with bad status %d\n",
	     child, WEXITSTATUS(status));
      exit(1);
    }
  }
}

void test_signal_task() {
  printf("\nTesting signal_task\n");
  fflush(stdout);
  fflush(stderr);
  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: fork failed\n");
    exit(1);
  } else if (child == 0) {
    if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
      exit(1);
    }
    sleep(3600);
    exit(0);
  } else {
    printf("Child task launched as %d\n", child);
    if (signal_user_task(username, child, SIGQUIT) != 0) {
      exit(1);
    }
    int status = 0;
    if (waitpid(child, &status, 0) == -1) {
      printf("FAIL: waitpid failed - %s\n", strerror(errno));
      exit(1);
    }
    if (!WIFSIGNALED(status)) {
      printf("FAIL: child wasn't signalled - %d\n", status);
      exit(1);
    }
    if (WTERMSIG(status) != SIGQUIT) {
      printf("FAIL: child was killed with %d instead of %d\n", 
	     WTERMSIG(status), SIGQUIT);
      exit(1);
    }
  }
}

void test_signal_task_group() {
  printf("\nTesting group signal_task\n");
  fflush(stdout);
  fflush(stderr);
  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: fork failed\n");
    exit(1);
  } else if (child == 0) {
    setpgrp();
    if (change_user(user_detail->pw_uid, user_detail->pw_gid) != 0) {
      exit(1);
    }
    sleep(3600);
    exit(0);
  }
  printf("Child task launched as %d\n", child);
  if (signal_user_task(username, child, SIGKILL) != 0) {
    exit(1);
  }
  int status = 0;
  if (waitpid(child, &status, 0) == -1) {
    printf("FAIL: waitpid failed - %s\n", strerror(errno));
    exit(1);
  }
  if (!WIFSIGNALED(status)) {
    printf("FAIL: child wasn't signalled - %d\n", status);
    exit(1);
  }
  if (WTERMSIG(status) != SIGKILL) {
    printf("FAIL: child was killed with %d instead of %d\n", 
	   WTERMSIG(status), SIGKILL);
    exit(1);
  }
}

void test_init_job() {
  printf("\nTesting init job\n");
  if (seteuid(0) != 0) {
    printf("FAIL: seteuid to root failed - %s\n", strerror(errno));
    exit(1);
  }
  FILE* creds = fopen(TEST_ROOT "/creds.txt", "w");
  if (creds == NULL) {
    printf("FAIL: failed to create credentials file - %s\n", strerror(errno));
    exit(1);
  }
  if (fprintf(creds, "secret key\n") < 0) {
    printf("FAIL: fprintf failed - %s\n", strerror(errno));
    exit(1);
  }
  if (fclose(creds) != 0) {
    printf("FAIL: fclose failed - %s\n", strerror(errno));
    exit(1);
  }
  FILE* job_xml = fopen(TEST_ROOT "/job.xml", "w");
  if (job_xml == NULL) {
    printf("FAIL: failed to create job file - %s\n", strerror(errno));
    exit(1);
  }
  if (fprintf(job_xml, "<jobconf/>\n") < 0) {
    printf("FAIL: fprintf failed - %s\n", strerror(errno));
    exit(1);
  }
  if (fclose(job_xml) != 0) {
    printf("FAIL: fclose failed - %s\n", strerror(errno));
    exit(1);
  }
  if (seteuid(user_detail->pw_uid) != 0) {
    printf("FAIL: failed to seteuid back to user - %s\n", strerror(errno));
    exit(1);
  }
  fflush(stdout);
  fflush(stderr);
  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: failed to fork process for init_job - %s\n", 
	   strerror(errno));
    exit(1);
  } else if (child == 0) {
    char *final_pgm[] = {"touch", "my-touch-file", 0};
    char* local_dirs = get_value("mapred.local.dir");
    if (initialize_job(username, local_dirs, "job_4", TEST_ROOT "/creds.txt", 
                       TEST_ROOT "/job.xml", final_pgm) != 0) {
      printf("FAIL: failed in child\n");
      exit(42);
    }
    // should never return
    exit(1);
  }
  int status = 0;
  if (waitpid(child, &status, 0) <= 0) {
    printf("FAIL: failed waiting for process %d - %s\n", child, 
	   strerror(errno));
    exit(1);
  }
  if (access(TEST_ROOT "/logs/userlogs/job_4", R_OK) != 0) {
    printf("FAIL: failed to create job log directory\n");
    exit(1);
  }
  char* job_dir = get_job_directory(TEST_ROOT "/local-1", username, "job_4");
  if (access(job_dir, R_OK) != 0) {
    printf("FAIL: failed to create job directory %s\n", job_dir);
    exit(1);
  }
  char buffer[100000];
  sprintf(buffer, "%s/jobToken", job_dir);
  if (access(buffer, R_OK) != 0) {
    printf("FAIL: failed to create credentials %s\n", buffer);
    exit(1);
  }
  sprintf(buffer, "%s/my-touch-file", job_dir);
  if (access(buffer, R_OK) != 0) {
    printf("FAIL: failed to create touch file %s\n", buffer);
    exit(1);
  }
  free(job_dir);
  job_dir = get_job_log_directory("job_4");
  if (access(job_dir, R_OK) != 0) {
    printf("FAIL: failed to create job log directory %s\n", job_dir);
    exit(1);
  }
  free(job_dir);
}

void test_run_task() {
  printf("\nTesting run task\n");
  if (seteuid(0) != 0) {
    printf("FAIL: seteuid to root failed - %s\n", strerror(errno));
    exit(1);
  }
  const char* script_name = TEST_ROOT "/task-script";
  FILE* script = fopen(script_name, "w");
  if (script == NULL) {
    printf("FAIL: failed to create script file - %s\n", strerror(errno));
    exit(1);
  }
  if (seteuid(user_detail->pw_uid) != 0) {
    printf("FAIL: failed to seteuid back to user - %s\n", strerror(errno));
    exit(1);
  }
  if (fprintf(script, "#!/bin/bash\n"
                     "touch foobar\n"
                     "exit 0") < 0) {
    printf("FAIL: fprintf failed - %s\n", strerror(errno));
    exit(1);
  }
  if (fclose(script) != 0) {
    printf("FAIL: fclose failed - %s\n", strerror(errno));
    exit(1);
  }
  fflush(stdout);
  fflush(stderr);
  char* task_dir = get_attempt_work_directory(TEST_ROOT "/local-1", 
					      username, "job_4", "task_1");
  char* local_dirs = get_value("mapred.local.dir");
  pid_t child = fork();
  if (child == -1) {
    printf("FAIL: failed to fork process for init_job - %s\n", 
	   strerror(errno));
    exit(1);
  } else if (child == 0) {
    if (run_task_as_user(username, local_dirs, "job_4", "task_1", 
                         task_dir, script_name) != 0) {
      printf("FAIL: failed in child\n");
      exit(42);
    }
    // should never return
    exit(1);
  }
  int status = 0;
  if (waitpid(child, &status, 0) <= 0) {
    printf("FAIL: failed waiting for process %d - %s\n", child, 
	   strerror(errno));
    exit(1);
  }
  if (access(TEST_ROOT "/logs/userlogs/job_4/task_1", R_OK) != 0) {
    printf("FAIL: failed to create task log directory\n");
    exit(1);
  }
  if (access(task_dir, R_OK) != 0) {
    printf("FAIL: failed to create task directory %s\n", task_dir);
    exit(1);
  }
  char buffer[100000];
  sprintf(buffer, "%s/foobar", task_dir);
  if (access(buffer, R_OK) != 0) {
    printf("FAIL: failed to create touch file %s\n", buffer);
    exit(1);
  }
  free(task_dir);
  task_dir = get_job_log_directory("job_4/task_1");
  if (access(task_dir, R_OK) != 0) {
    printf("FAIL: failed to create job log directory %s\n", task_dir);
    exit(1);
  }
  free(task_dir);
}

int main(int argc, char **argv) {
  LOGFILE = stdout;
  int my_username = 0;

  // clean up any junk from previous run
  if (system("chmod -R u=rwx " TEST_ROOT "; rm -fr " TEST_ROOT)) {
    perror("Warning, couldn't clean " TEST_ROOT);
    // but maybe it just didn't exist, so keep going.
  }
  
  if (mkdirs(TEST_ROOT "/logs/userlogs", 0755) != 0) {
    exit(1);
  }
  
  if (write_config_file(TEST_ROOT "/test.cfg") != 0) {
    exit(1);
  }
  read_config(TEST_ROOT "/test.cfg");

  create_tt_roots();

  if (getuid() == 0 && argc == 2) {
    username = argv[1];
  } else if ((username = getenv("TC_TEST_USERNAME")) == NULL) {
    username = strdup(getpwuid(getuid())->pw_name);
    my_username = 1;
  }
  set_tasktracker_uid(geteuid(), getegid());

  if (set_user(username)) {
    exit(1);
  }

  printf("\nStarting tests\n");

  printf("\nTesting get_user_directory()\n");
  test_get_user_directory();

  printf("\nTesting get_job_directory()\n");
  test_get_job_directory();

  printf("\nTesting get_attempt_directory()\n");
  test_get_attempt_directory();

  printf("\nTesting get_task_launcher_file()\n");
  test_get_task_launcher_file();

  printf("\nTesting get_job_log_dir()\n");
  test_get_job_log_dir();

  test_check_configuration_permissions();

  printf("\nTesting get_task_log_dir()\n");
  test_get_task_log_dir();

  printf("\nTesting delete_task()\n");
  test_delete_task();

  printf("\nTesting delete_job()\n");
  test_delete_job();

  test_delete_user();

  test_check_user();

  test_create_log_directory();

  test_delete_log_directory();

  // the tests that change user need to be run in a subshell, so that
  // when they change user they don't give up our privs
  run_test_in_child("test_signal_task", test_signal_task);
  run_test_in_child("test_signal_task_group", test_signal_task_group);

  // init job and run task can't be run if you aren't testing as root
  if (getuid() == 0) {
    // these tests do internal forks so that the change_owner and execs
    // don't mess up our process.
    test_init_job();
    test_run_task();
  }

  seteuid(0);
  run("rm -fr " TEST_ROOT);
  printf("\nFinished tests\n");

  if (my_username) {
    free(username);
  }
  free_configurations();
  return 0;
}
