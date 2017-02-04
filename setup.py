import sys

from setuptools import setup, find_packages

if sys.version_info < (3,):
    try:
        from lib3to2.build import build_py_3to2
    except ImportError:
        print('Please, install the "3to2" package')
        sys.exit(~0)

    cmdclass = {'build_py': build_py_3to2}
else:
    cmdclass = {}

setup(
    name='terry',
    version='0.0.2',
    description='Distributed task queue',
    url='https://github.com/werat/terry',
    author='Andy Yankovsky',
    author_email='weratt@gmail.com',
    license='MIT',
    classifiers=(
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
    ),
    cmdclass=cmdclass,
    packages=find_packages(exclude=['docs', 'tests', 'examples']),
    install_requires=['pymongo<=3.4.0'],
    tests_require=['pytest', 'pytest-timeout']
)
