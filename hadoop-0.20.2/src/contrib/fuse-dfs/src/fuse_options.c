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

#include "fuse_dfs.h"
#include "fuse_options.h"
#include <getopt.h>

#include "fuse_context_handle.h"

void print_options() {
  printf("options:\n"
	 "\tprotected=%s\n"
	 "\tserver=%s\n"
	 "\tport=%d\n"
	 "\tdebug=%d\n"
	 "\tread_only=%d\n"
	 "\tusetrash=%d\n"
	 "\tentry_timeout=%d\n"
	 "\tattribute_timeout=%d\n"
	 "\tprivate=%d\n"
	 "\trdbuffer_size=%d (KBs)\n", 
	 options.protected, options.server, options.port, options.debug,
	 options.read_only, options.usetrash, options.entry_timeout, 
	 options.attribute_timeout, options.private, 
	 (int)options.rdbuffer_size / 1024);
}

const char *program;

/** macro to define options */
#define DFSFS_OPT_KEY(t, p, v) { t, offsetof(struct options, p), v }

void print_usage(const char *pname)
{
  printf("USAGE: %s [debug] [--help] [--version] "
	 "[-oprotected=<colon_seped_list_of_paths] [rw] [-onotrash] "
	 "[-ousetrash] [-obig_writes] [-oprivate (single user)] [ro] "
	 "[-oserver=<hadoop_servername>] [-oport=<hadoop_port>] "
	 "[-oentry_timeout=<secs>] [-oattribute_timeout=<secs>] "
	 "[-odirect_io] [-onopoermissions] [-o<other fuse option>] "
	 "<mntpoint> [fuse options]\n", pname);
  printf("NOTE: debugging option for fuse is -debug\n");
}


/** keys for FUSE_OPT_ options */
enum
  {
    KEY_VERSION,
    KEY_HELP,
    KEY_USETRASH,
    KEY_NOTRASH,
    KEY_RO,
    KEY_RW,
    KEY_PRIVATE,
    KEY_BIGWRITES,
    KEY_DEBUG,
    KEY_INITCHECKS,
    KEY_NOPERMISSIONS,
    KEY_DIRECTIO,
  };

struct fuse_opt dfs_opts[] =
  {
    DFSFS_OPT_KEY("server=%s", server, 0),
    DFSFS_OPT_KEY("entry_timeout=%d", entry_timeout, 0),
    DFSFS_OPT_KEY("attribute_timeout=%d", attribute_timeout, 0),
    DFSFS_OPT_KEY("protected=%s", protected, 0),
    DFSFS_OPT_KEY("port=%d", port, 0),
    DFSFS_OPT_KEY("rdbuffer=%d", rdbuffer_size,0),

    FUSE_OPT_KEY("private", KEY_PRIVATE),
    FUSE_OPT_KEY("ro", KEY_RO),
    FUSE_OPT_KEY("debug", KEY_DEBUG),
    FUSE_OPT_KEY("initchecks", KEY_INITCHECKS),
    FUSE_OPT_KEY("nopermissions", KEY_NOPERMISSIONS),
    FUSE_OPT_KEY("big_writes", KEY_BIGWRITES),
    FUSE_OPT_KEY("rw", KEY_RW),
    FUSE_OPT_KEY("usetrash", KEY_USETRASH),
    FUSE_OPT_KEY("notrash", KEY_NOTRASH),
    FUSE_OPT_KEY("direct_io", KEY_DIRECTIO),
    FUSE_OPT_KEY("-v",             KEY_VERSION),
    FUSE_OPT_KEY("--version",      KEY_VERSION),
    FUSE_OPT_KEY("-h",             KEY_HELP),
    FUSE_OPT_KEY("--help",         KEY_HELP),
    FUSE_OPT_END
  };

int dfs_options(void *data, const char *arg, int key,  struct fuse_args *outargs)
{
  (void) data;

  switch (key) {
  case FUSE_OPT_KEY_OPT:
    INFO("Ignoring option %s", arg);
    return 1;
  case KEY_VERSION:
    INFO("%s %s\n", program, _FUSE_DFS_VERSION);
    exit(0);
  case KEY_HELP:
    print_usage(program);
    exit(0);
  case KEY_USETRASH:
    options.usetrash = 1;
    break;
  case KEY_NOTRASH:
    options.usetrash = 1;
    break;
  case KEY_RO:
    options.read_only = 1;
    break;
  case KEY_RW:
    options.read_only = 0;
    break;
  case KEY_PRIVATE:
    options.private = 1;
    break;
  case KEY_DEBUG:
    fuse_opt_add_arg(outargs, "-d");
    options.debug = 1;
    break;
  case KEY_INITCHECKS:
    options.initchecks = 1;
    break;
  case KEY_NOPERMISSIONS:
    options.no_permissions = 1;
    break;
  case KEY_DIRECTIO:
    options.direct_io = 1;
    break;
  case KEY_BIGWRITES:
#ifdef FUSE_CAP_BIG_WRITES
    fuse_opt_add_arg(outargs, "-obig_writes");
#endif
    break;
  default: {
    // try and see if the arg is a URI for DFS
    int tmp_port;
    char tmp_server[1024];

    if (!sscanf(arg,"dfs://%1024[a-zA-Z0-9_.-]:%d",tmp_server,&tmp_port)) {
      if (strcmp(arg,"ro") == 0) {
        options.read_only = 1;
      } else if (strcmp(arg,"rw") == 0) {
        options.read_only = 0;
      } else {
        INFO("Adding FUSE arg %s", arg);
        fuse_opt_add_arg(outargs, arg);
        return 0;
      }
    } else {
      options.port = tmp_port;
      options.server = strdup(tmp_server);
    }
  }
  }
  return 0;
}
