#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
General utility classes for the program
'''
import os
import sys
import csv
from collections import defaultdict

import find
import log
import tools as t
import config

# ~~~~ CUSTOM CLASSES ~~~~~~ #
class LoggedObject(object):
    '''
    Base class for an object with its own custom logger

    Requires an id to be passed
    extra_handlers should be a list of handlers to add to the logger
    '''
    def __init__(self, id, extra_handlers = None):
        self.id = str(id)
        # per-item logging setup & configuration; console logging only by default
        self.logger = log.build_logger(name = self.id)

        # add extra handlers if passed
        if extra_handlers != None:
            # self.logger.debug("extra_handlers: {0}".format(extra_handlers))
            for h in extra_handlers:
                if h != None:
                    self.logger.addHandler(h)

    def get_handler_paths(self, logger, types = ['FileHandler']):
        '''
        Get the paths to all handlers
        returns a dict of format {name: path}
        '''
        handlers = [x for x in log.get_all_handlers(logger = logger, types = types)]
        handler_paths = {}
        for h in handlers:
            name = h.get_name()
            handler_paths[name] = log.logger_filepath(logger = logger, handler_name = name)
        return(handler_paths)

    def log_handler_paths(self, logger, types = ['FileHandler']):
        '''
        Log the paths to all handlers
        '''
        handler_paths = self.get_handler_paths(logger = logger, types = types)
        for name, path in handler_paths.items():
            logger.info('Path to {0} log file: {1}'.format(name, path))

    def __repr__(self):
        return(self.id)
    def __str__(self):
        return(self.id)
    def __len__(self):
        return(len(self.id))
