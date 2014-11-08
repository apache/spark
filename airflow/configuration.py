import logging
import os

from ConfigParser import ConfigParser, NoOptionError, NoSectionError

class AirflowConfigParser(ConfigParser):
    NO_DEFAULT = object()
    _instance = None
    _config_paths = ['airflow.cfg']
    if 'AIRFLOW_CONFIG_PATH' in os.environ:
        _config_paths.append(os.environ['AIRFLOW_CONFIG_PATH'])
        logging.info("Config paths is " + str(_config_paths))
    print("Config paths is " + str(_config_paths))

    @classmethod
    def add_config_paths(cls, path):
        cls._config_paths.append(path)
        cls.reload()

    @classmethod
    def instance(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
            cls._instance.reload()

        return cls._instance

    @classmethod
    def reload(cls):
        loaded_obj = cls.instance().read(cls._config_paths)
        logging.info("the config object after loading is " + str(loaded_obj))
        return loaded_obj

    def get_with_default(self, method, section, option, default):
        try:
            return method(self, section, option)
        except (NoOptionError, NoSectionError):
            if default is AirflowConfigParser.NO_DEFAULT:
                raise
            return default

    def get(self, section, option, default=NO_DEFAULT):
        return self.get_with_default(ConfigParser.get, section, option, default)

    def getint(self, section, option, default=NO_DEFAULT):
        return self.get_with_default(ConfigParser.getint, section, option, default)

    def getboolean(self, section, option, default=NO_DEFAULT):
        return self.get_with_default(ConfigParser.getboolean, section, option, default)

    def set(self, section, option, value):
        if not ConfigParser.has_section(self, section):
            ConfigParser.add_section(self, section)
        return ConfigParser.set(self, section, option, value)

def getconf():
    return AirflowConfigParser.instance()
