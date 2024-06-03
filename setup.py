"""File for packaging netCBS."""

# based on https://github.com/pypa/sampleproject - MIT License

from setuptools import find_packages
from setuptools import setup

    
setup(
    name='netcbs',
    version='0.1',
    author='ODISSEI Social Data Science Team',
    description='Package to create aggregated variables from CBS network data (POPNET)',
    long_description='See https://github.com/sodascience/cbsnet for examples and usage',
    keywords='popnet cbs networks',
    license='GPL-3.0',
    url='https://github.com/sodascience/netcbs',
    packages=find_packages(exclude=['data', 'docs', 'tests', 'examples']),
    install_requires=[
        "pandas",
        "polars",
        "numpy",
    ]
)