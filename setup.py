from setuptools import setup, find_packages
from codecs import open
from os import path
import sys

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


install_requires = [
    'sqlalchemy',
    'python-dateutil'
]

needs_sphinx = {'build_sphinx', 'upload_docs'}.intersection(sys.argv)
sphinx = ['sphinx'] if needs_sphinx else []

setup(
    name='qsqla',
    description='qSQLA is a query builder for SQLAlchemy Core Selectables ',
    long_description=long_description,
    url='https://github.com/blue-yonder/qsqla',
    download_url='https://github.com/blue-yonder/qsqla/tarball/',
    license='BSD',
    classifiers=[
      'Development Status :: 3 - Alpha',
      'Intended Audience :: Developers',
      'Programming Language :: Python :: 3',
    ],
    keywords='',
    use_scm_version=True,
    packages=find_packages(exclude=['docs', 'tests*']),
    include_package_data=True,
    setup_requires=['setuptools_scm'] + sphinx,
    author='Peter Hoffmann',
    install_requires=install_requires,
    author_email='peter.hoffmann@blue-yonder.com'
)
