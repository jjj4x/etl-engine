from pathlib import Path
from os import environ

from setuptools import setup, find_packages

ROOT_PATH = Path(__file__).parent.absolute()
RELEASE = '1.0.0{branch_modifier}{build_number}'
BUILD_NUMBER = environ.get('BUILD_NUMBER', 0)
BRANCH_NAME = environ.get('BRANCH_NAME', 'dev')

if BRANCH_NAME == 'master':
    VERSION = RELEASE.format(branch_modifier='', build_number='')
else:
    VERSION = RELEASE.format(branch_modifier='.dev', build_number=BUILD_NUMBER)


if __name__ == '__main__':
    with open(ROOT_PATH / 'README.rst', encoding='utf8') as fd:
        long_description = fd.read()

    with open(ROOT_PATH / 'requirements.txt', encoding='utf8') as fd:
        install_requires = [line for line in fd.readlines() if not line.startswith('#')]

    with open(ROOT_PATH / 'requirements.dev.txt', encoding='utf8') as fd:
        development_require = [line for line in fd.readlines() if not line.startswith('#')]

    setup(
        name='etl-engine',
        url='https://github.com/jjj4x/etl-engine.git',
        version=VERSION,
        author='Max Preobrazhensky',
        author_email='max.preobrazhensky@gmail.com',
        description='Python ETL Engine (etl_engine) on Spark.',
        long_description=long_description,
        python_requires='>=3.7',
        install_requires=install_requires,
        extras_require={'development': development_require},
        include_package_data=True,
        package_dir={'': 'src'},
        packages=find_packages(where='src'),
    )
