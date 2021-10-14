#!/usr/bin/env python3
""" Python PyPI (setuptools) for ipo """
import pathlib
import re
from setuptools import setup  # type: ignore

# Our path
PWD = pathlib.Path(__file__).parent

# Get the long description from the readme.
README = (PWD / "README.md").read_text()

# Read requirements from file into array.
REQ = [line for line in map(lambda l: l.strip(), (PWD / "requirements.txt").read_text().split('\n'))
       if len(line) > 0 and not line.startswith('#')]


def get_version():
    """ Read the module version from __init__.py """
    lines = list(filter(lambda l: l.startswith("__version__"),
                        (PWD / "ipo/__init__.py").read_text().split("\n")))
    if len(lines) != 1:
        raise Exception("Version string missing?")
    line = lines[0]

    match = re.search(r"\"(\d+\.\d+.\d+)\"", line)
    if not match:
        raise Exception("Version numbering not in expected format, please either correct regex or the version string.")
    return match.group(1)


VERSION = get_version()


# This call to setup() does all the work
setup(
    name="ipo",
    version=VERSION,
    packages=["ipo"],
    description="Intelligen Container (ICON) python orchestrator",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/randombtree/ipo",
    author="Roger Blomgren",
    author_email="roger.blomgren@iki.fi",
    classifiers=[
        "Development Status :: 1 - Planning",
    ],
    include_package_data=True,
    install_requires=REQ,
    entry_points={
        "console_scripts": [
            "ipo=ipo.command:main",
        ]
    },
)
