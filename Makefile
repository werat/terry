build_py2:
	virtualenv --python=python2.7 build-venv
	build-venv/bin/pip install 3to2
	build-venv/bin/python setup.py bdist_wheel
	rm -rf build-venv build .egg terry.egg-info

build_py3:
	virtualenv --python=python3.6 build-venv
	build-venv/bin/python setup.py bdist_wheel
	rm -rf build-venv build .egg terry.egg-info

build: build_py2 build_py3

publish: build
	twine upload dist/*

cleanup:
	rm -rf dist
