import os
from setuptools import setup, find_packages

REQUIREMENTS_FILE       =       'requirements.txt'
with open(REQUIREMENTS_FILE,"r") as f:
        requirements    =       f.read().rstrip().split("\n")
        
VERSION = '0.0.3' 
DESCRIPTION = 'monstruito packages analisis'

# Setting up
setup(
       # the name must match the folder name 'verysimplemodule'
        name="monstry", 
        version=VERSION,
        author="Anderson Ocaña",
        author_email="andru.ocatorres@gmail.com",
        description=DESCRIPTION,
        packages=find_packages(),
        install_requires=requirements,
        keywords=['python', 'first package'],
)
