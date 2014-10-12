import os
from distutils.core import setup
from pip.req import parse_requirements

req_filepath = os.path.join(os.path.dirname(__file__), "/requirements.txt")
install_reqs = parse_requirements(req_filepath)

setup(
    name='flux',
    version='0.1',
    packages=['flux'],
    install_requires=install_reqs,
    author='Maxime Beauchemin',
    author_email='maximebeauchemin@gmail.com',
)

