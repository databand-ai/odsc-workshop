from setuptools import find_packages
from distutils.core import setup

setup(
    name='odsc-workshop-examples',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        "sklearn",
        "matplotlib",
        "pandas<1.0.0,>=0.17.1",
        "nbformat",
        "plotly",
        "dbnd"
    ]
)