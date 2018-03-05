SHELL:=/bin/bash

# setup virtual environment for Python 2.7
venv/bin/activate:
	export PYTHONPATH= && \
	virtualenv venv --no-site-packages

activate: venv/bin/activate
	ln -fs venv/bin/activate activate

install: venv/bin/activate activate
	export PYTHONPATH= && \
	source venv/bin/activate && \
	pip install -r requirements.txt

# cleanup
clean:
	rm -f fixtures/foo*_*
	rm -f fixtures/test_dump.csv
