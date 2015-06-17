import imp
import inspect
import logging
import os

from airflow.configuration import conf


plugins_folder = conf.get('core', 'plugins_folder')
if not plugins_folder:
    plugins_folder = conf.get('core', 'airflow_home') + '/plugins'
plugins_folder = os.path.expanduser(plugins_folder)

plugin_modules = []
# Crawl through the plugins folder to find pluggable_classes
templates_dirs = []

for root, dirs, files in os.walk(plugins_folder):
    if os.path.basename(root) == 'templates':
        templates_dirs.append(root)
    for f in files:
        try:
            filepath = os.path.join(root, f)
            if not os.path.isfile(filepath):
                continue
            mod_name, file_ext = os.path.splitext(
                os.path.split(filepath)[-1])
            if file_ext != '.py':
                continue
            m = imp.load_source(mod_name, filepath)
            plugin_modules.append(m)
        except Exception() as e:
            logging.exception(e)
            logging.error('Failed to import plugin ' + filepath)


def register_templates_folders(app):
    from jinja2 import ChoiceLoader, FileSystemLoader
    new_loaders = [FileSystemLoader(s) for s in templates_dirs]
    app.jinja_env.loader = ChoiceLoader([app.jinja_env.loader] + new_loaders)


def get_plugins(baseclass, expect_class=True):
    """
    Set expect_class=False if you want instances of baseclass
    """
    # Late Imports to aoid circular imort
    plugins = []
    for m in plugin_modules:
        for obj in m.__dict__.values():
            if ((
                    expect_class and inspect.isclass(obj) and
                    issubclass(obj, baseclass) and
                    obj is not baseclass)
                    or
                    (not expect_class and isinstance(obj, baseclass))):
                plugins.append(obj)
    return plugins
