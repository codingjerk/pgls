#!/usr/bin/env python3

from setuptools import find_packages, setup

setup(
    name="pgls",
    packages=find_packages(),
    entry_points={
        "console_scripts": ["pgls=pgls:main"],
    },
    version="1.0.1",
    description="CLI utility to display postgres database information as a tree.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/codingjerk/pgls",
    author="Denis Gruzdev",
    author_email="codingjerk@gmail.com",
    license="MIT",

    install_requires=[
        "asyncpg==0.21.0",
        "click==7.1.2",
        "colorama==0.4.3",
    ],
    setup_requires=[
    ],
)
