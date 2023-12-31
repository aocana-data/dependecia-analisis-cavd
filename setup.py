import os
from setuptools import setup, find_packages

required = [
"asttokens==2.2.1",
"backcall==0.2.0",
"cffi==1.15.1",
"comm==0.1.2",
"cryptography!=40.0.0",
"debugpy==1.6.6",
"decorator==5.1.1",
"et-xmlfile==1.1.0",
"executing==1.2.0",
"greenlet==2.0.2",
"ipykernel==6.21.3",
"ipython==8.11.0",
"jedi==0.18.2",
"jupyter_client==8.0.3",
"jupyter_core==5.2.0",
"matplotlib-inline==0.1.6",
"nest-asyncio==1.5.6",
"numpy==1.24.2",
"openpyxl==3.1.2",
"packaging==23.0",
"pandas==1.5.3",
"parso==0.8.3",
"pexpect==4.8.0",
"pickleshare==0.7.5",
"platformdirs==3.1.1",
"prompt-toolkit==3.0.38",
"protobuf==3.20.3",
"psutil==5.9.4",
"psycopg2-binary==2.9.5",
"ptyprocess==0.7.0",
"pure-eval==0.2.2",
"pycparser==2.21",
"Pygments==2.14.0",
"PyMySQL==1.0.3",
"python-dateutil==2.8.2",
"python-dotenv==1.0.0",
"pytz==2022.7.1",
"PyYAML==6.0",
"pyzmq==25.0.1",
"six==1.16.0",
"SQLAlchemy==2.0.0",
"stack-data==0.6.2",
"tornado==6.2",
"traitlets==5.9.0",
"typing_extensions==4.5.0",
"wcwidth==0.2.6",
"matplotlib"
]

VERSION = '0.0.1' 
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
        install_requires=required,
        keywords=['python', 'first package'],
)
