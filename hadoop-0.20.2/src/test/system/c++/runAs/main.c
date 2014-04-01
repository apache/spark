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

#include "runAs.h"

/**
 * The binary would be accepting the command of following format:
 * cluster-controller user hostname hadoop-daemon.sh-command
 */
int main(int argc, char **argv) {
  int errorcode;
  char *user;
  char *hostname;
  char *command;
  struct passwd user_detail;
  int i = 1;
  /*
   * Minimum number of arguments required for the binary to perform.
   */
  if (argc < 4) {
    fprintf(stderr, "Invalid number of arguments passed to the binary\n");
    return INVALID_ARGUMENT_NUMER;
  }

  user = argv[1];
  if (user == NULL) {
    fprintf(stderr, "Invalid user name\n");
    return INVALID_USER_NAME;
  }

  if (getuserdetail(user, &user_detail) != 0) {
    fprintf(stderr, "Invalid user name\n");
    return INVALID_USER_NAME;
  }

  if (user_detail.pw_gid == 0 || user_detail.pw_uid == 0) {
      fprintf(stderr, "Cannot run tasks as super user\n");
      return SUPER_USER_NOT_ALLOWED_TO_RUN_COMMANDS;
  }

  hostname = argv[2];
  command = argv[3];
  return process_controller_command(user, hostname, command);
}
