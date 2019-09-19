SHELL:=/bin/bash

UNAME:=$(shell uname)
export PATH:=$(CURDIR)/conda/bin:$(PATH)
unexport PYTHONPATH
unexport PYTHONHOME

ifeq ($(UNAME), Darwin)
CONDASH:=Miniconda2-4.5.4-MacOSX-x86_64.sh
endif

ifeq ($(UNAME), Linux)
CONDASH:=Miniconda2-4.5.4-Linux-x86_64.sh
endif

CONDAURL:=https://repo.continuum.io/miniconda/$(CONDASH)

conda:
	@echo ">>> Setting up conda..."
	wget "$(CONDAURL)" && \
	bash "$(CONDASH)" -b -p conda && \
	rm -f "$(CONDASH)"

conda-install: conda
	conda install -y -c anaconda \
	pyyaml=5.1

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

test:
	python test.py

bash:
	bash
