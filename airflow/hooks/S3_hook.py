import logging
import json
import re
import ConfigParser
from urlparse import urlparse

from airflow.models import Connection
from airflow.configuration import conf
from airflow import settings

from airflow.hooks.base_hook import BaseHook

import boto
from boto.s3.connection import S3Connection
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
        session = settings.Session()
        db = session.query(
            Connection).filter(
                Connection.conn_id == s3_conn_id)
        if db.count() == 0:
            raise Exception("The conn_id you provided isn't defined")
        else:
            db = db.all()[0]
        # Get S3 credentials
        self.profile = None
        try:
            extra_params = json.loads(db.extra)
            s3_config_format = extra_params['s3_config_format']
            s3_config_file = extra_params['s3_config_file']
            if 'profile' in extra_params:
                self.profile = extra_params['profile']
        except TypeError as e:
            raise Exception("S3 connection needs to set config params in extra")
        except KeyError as e:
            raise Exception("S3 connection definition needs to include"
                            "{p} in extra".format(p=e.message))
        a_key, s_key = _parse_s3_config(s3_config_file, s3_config_format,
                                        self.profile)
        self.connection = S3Connection(aws_access_key_id=a_key,
                                       aws_secret_access_key=s_key)
        session.commit()
        session.close()

    def get_conn(self):
        '''
        Returns the boto S3Connection object.
        '''
        return self.connection

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
        return prefix_names if prefix_names !=[] else None

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
