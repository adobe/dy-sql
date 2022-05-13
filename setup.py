import importlib.machinery
import os
import subprocess
import types
from setuptools import setup, find_packages


BASE_VERSION = '1.13'
SOURCE_DIR = os.path.dirname(
    os.path.abspath(__file__)
)
DYSQL_DIR = os.path.join(SOURCE_DIR, 'dysql')
VERSION_FILE = os.path.join(DYSQL_DIR, 'version.py')
HEADER_FILE = os.path.join(SOURCE_DIR, '.pylint-license-header')


def get_version():
    """
    Call out to the git command line to get the current commit "number".
    """
    if os.path.exists(VERSION_FILE):
        # Read version from file
        loader = importlib.machinery.SourceFileLoader('dysql_version', VERSION_FILE)
        version_mod = types.ModuleType(loader.name)
        loader.exec_module(version_mod)
        existing_version = version_mod.__version__  # pylint: disable=no-member
        print(f'Using existing dysql version: {existing_version}')
        return existing_version

    # Generate the version from the base version and the git commit number, then store it in the file
    try:
        cmd = subprocess.Popen(
            args=[
                'git',
                'rev-list',
                '--count',
                'HEAD',
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding='utf8',
        )
        stdout = cmd.communicate()[0]
        output = stdout.strip()
        if cmd.returncode == 0:
            new_version = '{0}.{1}'.format(BASE_VERSION, output)
            print(f'Setting version to {new_version}')

            # write the version file
            if os.path.exists(DYSQL_DIR):
                with open(HEADER_FILE, 'r', encoding='utf8') as fobj:
                    header = fobj.read()
                with open(VERSION_FILE, 'w', encoding='utf8') as fobj:
                    fobj.write(f"{header}\n__version__ = '{new_version}'\n")
            return new_version
    except Exception as exc:
        print(f'Could not generate version from git commits: {exc}')
    # If all else fails, use development version
    return f'{BASE_VERSION}.DEVELOPMENT'


with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as fobj:
    long_description = fobj.read().strip()


setup(
    name='dy-sql',
    version=get_version(),
    license='MIT',
    description='Dynamically runs SQL queries and executions.',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    author='Adobe',
    author_email='noreply@adobe.com',
    url='https://github.com/adobe/dy-sql',
    platforms=['Any'],
    packages=find_packages(exclude=('*test*',)),
    zip_safe=False,
    install_requires=(
        'sqlalchemy>=1.4',
    ),
    extras_require={
        'pydantic': ['pydantic>=1.8.2,<2'],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],

)
