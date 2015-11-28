# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
# Ported to Airflow by Bolke de Bruin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Light-weight remote execution library and utilities.

This is a port of Luigi's ssh implementation. All credits go there.

Using this hook (which is just a convenience wrapper for subprocess),
is created to let you stream data from a remotely stored file.

As a bonus, :class:`SSHHook` also provides a really cool feature that let's you
set up ssh tunnels super easily using a python context manager (there is an example
in the integration part of unittests).

This can be super convenient when you want secure communication using a non-secure
protocol or circumvent firewalls (as long as they are open for ssh traffic).
"""

import subprocess

from airflow.hooks.base_hook import BaseHook
from airflow import AirflowException

import logging

class SSHHook(BaseHook):
    def __init__(self, conn_id='ssh_default'):
        conn = self.get_connection(conn_id)
        self.key_file = conn.extra_dejson.get('key_file', None)
        self.connect_timeout = conn.extra_dejson.get('connect_timeout', None)
        self.no_host_key_check = conn.extra_dejson.get('no_host_key_check', False)
        self.tty = conn.extra_dejson.get('tty', False)
        self.sshpass = conn.extra_dejson.get('sshpass', False)
        self.conn = conn

    def get_conn(self):
        pass

    def _host_ref(self):
        if self.conn.login:
            return "{0}@{1}".format(self.conn.login, self.conn.host)
        else:
            return self.conn.host

    def _prepare_command(self, cmd):
        connection_cmd = ["ssh", self._host_ref(), "-o", "ControlMaster=no"]
        if self.sshpass:
            connection_cmd = ["sshpass", "-e"] + connection_cmd
        else:
            connection_cmd += ["-o", "BatchMode=yes"] # no password prompts

        if self.conn.port:
            connection_cmd.extend(["-p", self.conn.port])

        if self.connect_timeout:
            connection_cmd += ["-o", "ConnectionTimeout={}".format(self.connect_timeout)]

        if self.no_host_key_check:
            connection_cmd += ["-o", "UserKnownHostsFile=/dev/null",
                               "-o", "StrictHostKeyChecking=no"]

        if self.key_file:
            connection_cmd.extend("-i", self.key_file)

        if self.tty:
            connection_cmd.append("-t")

        logging.debug("SSH cmd: {} ".format(connection_cmd + cmd))

        return connection_cmd + cmd

    def _Popen(self, cmd, **kwargs):
        """
        Remote Popen
        :param cmd:
        :param kwargs:
        :return:
        """
        prefixed_cmd = self._prepare_command(cmd)
        return subprocess.Popen(prefixed_cmd, **kwargs)

    def check_output(self, cmd):
        p = self._Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, stderr = p.communicate()

        if p.returncode != 0:
            # I like this better: RemoteCalledProcessError(p.returncode, cmd, self.host, output=output)
            raise AirflowException("Cannot execute {} on {}. Error code is: {}. Output: {}, Stderr: {}".format(
                                   cmd, self.conn.host, p.returncode, output, stderr))

        return output

    def tunnel(self, local_port, remote_port=None, remote_host="localhost"):
        tunnel_host = "{0}:{1}:{2}".format(local_port, remote_host, remote_port)
        proc = self._Popen(["-L", tunnel_host, "echo -n ready && cat"],
                           stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                           )

        ready = proc.stdout.read(5)
        assert ready == b"ready", "Did not get 'ready' from remote"
        yield
        proc.communicate()
        assert proc.returncode == 0, "Tunnel process did unclean exit (returncode {}".format(proc.returncode)

