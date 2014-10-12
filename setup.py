import os
from distutils.core import setup
from pip.req import parse_requirements

# Reading the requirements from pip's requirements.txt
reqs = (str(ir.req) for ir in parse_requirements('requirements.txt'))
print reqs

setup(
    name='flux',
    version='0.1',
    packages=[
        'flux', 
        'flux.operators', 
        'flux.executors',
        'flux.hooks',
        'flux.www',
    ],
    scripts=['flux/bin/flux'],
    install_requires=reqs,
    author='Maxime Beauchemin',
    author_email='maximebeauchemin@gmail.com',
    url='https://github.com/mistercrunch/Flux',
)
