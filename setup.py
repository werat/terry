from setuptools import setup, find_packages

setup(
    name='terry',
    version='0.0.1',
    description='',
    url='https://github.com/werat/terry',
    author='Andy Yankovsky',
    author_email='weratt@gmail.com',
    license='MIT',
    packages=find_packages(exclude=['docs', 'tests', 'examples']),
    install_requires=['pymongo<=3.4.0']
)
