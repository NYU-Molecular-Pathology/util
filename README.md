[![Build Status](https://travis-ci.org/NYU-Molecular-Pathology/util.svg?branch=master)](https://travis-ci.org/NYU-Molecular-Pathology/util)
# util

Utility modules for Python, for use with other Python programs.

# Features

## Logging

The `log` submodule contains many functions for building and interacting with Python `logging` objects. Static logging configurations can be loaded from the `logging.yml` file, and the default location for log output is typically set to a local `logs` directory. Additionally, the `classes` submodule contains the `LoggedObject` class which can be used to create objects which have their own logging instances.

## Email

The `mutt` submodule is a flexible wrapper to the `mutt` system program, and can be used to send emails. A common use-case is send the contents of a submodule or object's log file as the body of an email to users as a notification of program completion. File attachments can also be sent, allowing you to also send some of the files created by your program with the email.

## Qsub

The `qsub` submodule includes the `Job` class for submitting jobs to a compute cluster and monitoring them for completion. Currently configured for the phoenix system at NYULMC running SGE. 

## Git

The `git` submodule contains functions for interacting with `git` installed on the system. For example, it is possible to check the current repository's branch information, in case you want prevent the program from running if not on the `master` branch. 

## Find

The `find` submodule contains the `find` function which can be used to search the system for desired files or directories. This function has been modeled off of the standard GNU `find` program and supports multiple inclusion and exclusion patterns, and search depth limits, among others. 

# Software
Designed and tested with Python 2.7

Designed for use on Linux system with Sun Grid Engine (SGE) installed

Some modules may make system calls relying on standard GNU utilities

# Credits

[`sh.py`](https://github.com/amoffat/sh) is used as an included dependency.
