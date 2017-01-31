.PHONY: build publish

build:
	# build for python2 and cleanup
	python2 setup.py bdist_wheel
	rm -rf build .egg terry.egg-info
	# build for python3 and cleanup
	python3 setup.py bdist_wheel
	rm -rf build .egg terry.egg-info

publish: build
	twine upload dist/*
