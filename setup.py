from os import path
from setuptools import setup

DIR = path.dirname(path.abspath(__file__))
INSTALL_PACKAGES = open(path.join(DIR, 'requirements.txt')).read().splitlines()


setup(
    name='zdutil',
    packages=['zdutil'],
    description="ZigData's data science library",
    install_requires=INSTALL_PACKAGES,
    package_data={'zdutil': ['*.ini']},
    include_package_data=True,
    version='0.0.1',
    url='http://github.com/ziggyzacks/zdutil'
)
