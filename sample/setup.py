#!/usr/bin/env python3
""" Python PyPI (setuptools) for ipo sample server """
import pathlib
from setuptools import setup

# Our path
PWD = pathlib.Path(__file__).parent

# Get the long description from the readme.
README = (PWD / "README.md").read_text()

# Read requirements from file into array.
REQ = [line for line in map(lambda l: l.strip(), (PWD / "requirements.txt").read_text().split('\n'))
       if len(line) > 0 and not line.startswith('#')]

VERSION = '0.0.1'


# This call to setup() does all the work
setup(
    name="iconsrv",
    version=VERSION,
    packages=["iconsrv"],
    description="Intelligen Container (ICON) sample application",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/randombtree/ipo",
    author="Roger Blomgren",
    author_email="roger.blomgren@iki.fi",
    classifiers=[
        "Development Status :: 1 - Planning",
    ],
    include_package_data=True,
    # package_data={
    #     "": ["*.txt", "*.md"],
    # },
    install_requires=REQ,
    entry_points={
        "console_scripts": [
            "iconsrv=iconsrv.main:main",
        ]
    },
)
