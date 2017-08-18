#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Functions for finding files and dirs

tested with python 2.7
'''
import logging
logger = logging.getLogger("git")
logger.debug("loading git module")

def parse_git(attribute):
    '''
    Check the current git repo for one of the following items
    attribute = "hash"
    attribute = "hash_short"
    attribute = "branch"
    '''
    import sys
    import subprocess
    command = None
    if attribute == "hash":
        command = ['git', 'rev-parse', 'HEAD']
    elif attribute == "hash_short":
        command = ['git', 'rev-parse', '--short', 'HEAD']
    elif attribute == "branch":
        command = ['git', 'rev-parse', '--abbrev-ref', 'HEAD']
    if command != None:
        try:
            return(subprocess.check_output(command).strip()) # python 2.7+
        except subprocess.CalledProcessError:
            logger.error('Git branch is not configured. Exiting script.')
            sys.exit()

def print_iter(iterable):
    '''
    basic printing of every item in an iterable object
    '''
    for item in iterable: logger.debug(item)

def validate_branch(allowed = ('master', 'production')):
    import sys
    import subprocess
    try:
        current_branch = parse_git(attribute = "branch")
        if current_branch not in allowed:
            logger.error("Current branch is not allowed! Branch is: {0}.".format(current_branch))
            logger.error("Allowed branches are:")
            for item in iterable: logger.error(item)
            logger.error("Exiting...")
            sys.exit()
    except subprocess.CalledProcessError:
        logger.error('Git branch is not configured. Exiting script.')
        sys.exit()
