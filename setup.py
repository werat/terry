from setuptools import setup, find_packages

setup(
    name='avery',
    version='0.0.1',
    description='',
    url='https://github.com/werat/avery',
    author='Andy Yankovsky',
    author_email='weratt@gmail.com',
    license='MIT',
    packages=find_packages(exclude=['docs', 'tests']),
    install_requires=['pymongo<=3.4.0']
)
