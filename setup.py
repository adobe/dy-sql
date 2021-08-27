from os.path import join, dirname
from setuptools import setup, find_packages


with open(join(dirname(__file__), 'dysql/VERSION')) as fobj:
    version = fobj.read().strip()
with open(join(dirname(__file__), 'README.rst')) as fobj:
    long_description = fobj.read().strip()

setup(
    name='dy-sql',
    version=version,
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
