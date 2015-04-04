import logging
import json
import re
import ConfigParser
from urlparse import urlparse

from airflow.models import Connection
from airflow import settings

from airflow.hooks.base_hook import BaseHook

import boto
from boto.s3.connection import S3Connection
from boto.sts import STSConnection
boto.set_stream_logger('boto')
logging.getLogger("boto").setLevel(logging.INFO)


def _parse_s3_config(config_file_name, config_format='boto', profile=None):
    """
    Parses a config file for s3 credentials. Can currently
    parse boto, s3cmd.conf and AWS SDK config formats

    :param config_file_name: path to the config file
    :type config_file_name: str
    :param config_format: config type. One of "boto", "s3cmd" or "aws".
        Defaults to "boto"
    :type config_format: str
    :param profile: profile name in AWS type config file
    :type profile: str
    """
    Config = ConfigParser.ConfigParser()
    if Config.read(config_file_name):
        sections = Config.sections()
    else:
        raise Exception("Couldn't read {0}".format(config_file_name))
    # Setting option names depending on file format
    conf_format = config_format.lower()
    if conf_format == 'boto':
        if profile is not None and 'profile ' + profile in sections:
            cred_section = 'profile ' + profile
        else:
            cred_section = 'Credentials'
    elif conf_format == 'aws' and profile is not None:
        cred_section = profile
    else:
        cred_section = 'default'
    # Option names
    if conf_format in ('boto', 'aws'):
        key_id_option = 'aws_access_key_id'
        secret_key_option = 'aws_secret_access_key'
        # security_token_option = 'aws_security_token'
    else:
        key_id_option = 'access_key'
        secret_key_option = 'secret_key'
    # Actual Parsing
    if cred_section not in sections:
        raise Exception("This config file format is not recognized")
    else:
        try:
            access_key = Config.get(cred_section, key_id_option)
            secret_key = Config.get(cred_section, secret_key_option)
        except:
            logging.warning("Option Error in parsing s3 config file")
            raise
        return (access_key, secret_key)


class S3Hook(BaseHook):
    '''
    Interact with S3. This class is a wrapper around the boto library.
    '''
    def __init__(
            self,
            s3_conn_id='s3_default'):
        self.s3_conn_id = s3_conn_id
        self.s3_conn = self.get_connection(s3_conn_id)
        self.profile = None
        self._sts_conn_required = False
        try:
            self.extra_params = json.loads(self.s3_conn.extra)
            self.s3_config_format = self.extra_params['s3_config_format']
            self.s3_config_file = self.extra_params['s3_config_file']
            if 'profile' in self.extra_params:
                self.profile = self.extra_params['profile']
            self._sts_conn_required = 'aws_account_id' in self.extra_params
            if self._sts_conn_required:
                self.aws_account_id = self.extra_params['aws_account_id']
                self.aws_iam_role = self.extra_params['aws_iam_role']
                self.role_arn = "arn:aws:iam::" + self.aws_account_id + ":role/"
                self.role_arn += self.aws_iam_role
        except TypeError as e:
            raise Exception("S3 connection needs to set config params in extra")
        except KeyError as e:
            raise Exception("S3 connection definition needs to include"
                            "{p} in extra".format(p=e.message))
        self.connection = self.get_conn()

    def __getstate__(self):
        pickled_dict = dict(self.__dict__)
        del pickled_dict['connection']
        return pickled_dict

    def __setstate__(self, d):
        self.__dict__.update(d)
        self.__dict__['connection'] = self.get_conn()

    def get_conn(self):
        '''
        Returns the boto S3Connection object.
        '''
        _s3_conn = self.get_connection(self.s3_conn_id)
        a_key, s_key = _parse_s3_config(self.s3_config_file,
                                        self.s3_config_format,
                                        self.profile)
        if self._sts_conn_required:
            sts_connection = STSConnection(aws_access_key_id=a_key,
                                           aws_secret_access_key=s_key,
                                           profile_name=self.profile)
            assumed_role_object = sts_connection.assume_role(
                role_arn=self.role_arn,
                role_session_name="Airflow_" + self.s3_conn_id
                )
            creds = assumed_role_object.credentials
            connection = S3Connection(
                aws_access_key_id=creds.access_key,
                aws_secret_access_key=creds.secret_key,
                security_token=creds.session_token
                )
        else:
            connection = S3Connection(aws_access_key_id=a_key,
                                      aws_secret_access_key=s_key)
        return connection

    def check_for_bucket(self, bucket_name):
        '''
        Check if bucket_name exists.

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        '''
        return self.connection.lookup(bucket_name) is not None

    def get_bucket(self, bucket_name):
        '''
        Returns a boto.s3.bucket object

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        '''
        return self.connection.get_bucket(bucket_name)

    def list_keys(self, bucket_name, prefix='', delimiter=''):
        '''
        Lists keys in a bucket under prefix and not containing delimiter

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param prefix: a key prefix
        :type prefix: str
        :param delimiter: the delimiter marks key hierarchy.
        :type delimiter: str
        '''
        b = self.get_bucket(bucket_name)
        keylist = list(b.list(prefix=prefix, delimiter=delimiter))
        return [k.name for k in keylist] if keylist != [] else None

    def list_prefixes(self, bucket_name, prefix='', delimiter=''):
        '''
        Lists prefixes in a bucket under prefix

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param prefix: a key prefix
        :type prefix: str
        :param delimiter: the delimiter marks key hierarchy.
        :type delimiter: str
        '''
        b = self.get_bucket(bucket_name)
        plist = b.list(prefix=prefix, delimiter=delimiter)
        prefix_names = [p.name for p in plist
                          if isinstance(p, boto.s3.prefix.Prefix)]
        return prefix_names if prefix_names != [] else None

    def check_for_key(self, key, bucket_name=None):
        '''
        Checks that a key exists in a bucket
        '''
        if bucket_name is None:
            parsed_url = urlparse(key)
            if parsed_url.netloc == '':
                raise Exception('Please provide a bucket_name')
            else:
                bucket_name = parsed_url.netloc
                key = parsed_url.path
        bucket = self.get_bucket(bucket_name)
        return bucket.get_key(key) is not None

    def check_for_prefix(self, bucket_name, prefix, delimiter):
        '''
        Checks that a prefix exists in a bucket
        '''
        prefix = prefix + delimiter if prefix[-1] != delimiter else prefix
        prefix_split = re.split(r'(\w+[{d}])$'.format(d=delimiter), prefix, 1)
        previous_level = prefix_split[0]
        leaf_prefix = prefix_split[1]
        plist = self.list_prefixes(bucket_name, previous_level, delimiter)
        return False if plist is None else prefix in plist
