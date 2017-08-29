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


class AnalysisItem(LoggedObject):
    '''
    Base class for objects associated with a data analysis
    '''
    def __init__(self, id, extra_handlers = None):
        LoggedObject.__init__(self, id = id, extra_handlers = extra_handlers)
        self.id = id
        # a dictionary of files associated with the item
        self.files = defaultdict(list)
        # a dictionary of dirs associated with the item
        self.dirs = defaultdict(list)

    def list_none(self, l):
        '''
        return None for an empty list, or the first element of a list
        convenience function for dealing with object's file lists
        '''
        if len(l) == 0:
            return(None)
        elif len(l) > 0:
            return(l[0])

    def set_dir(self, name, path):
        '''
        Add a single dir to the analysis object's 'dirs' dict
        name = dict key
        path = dict value
        '''
        if isinstance(path, str):
            self.dirs[name] = [os.path.abspath(path)]
        else:
            self.dirs[name] = [os.path.abspath(p) for p in path]

    def set_dirs(self, name, paths_list):
        '''
        Add dirs to the analysis object's 'dirs' dict
        name = dict key
        paths_list = list of file paths
        '''
        self.set_dir(name = name, path = paths_list)

    def set_file(self, name, path):
        '''
        Add a single file to the analysis object's 'files' dict
        name = dict key
        path = dict value
        '''
        if isinstance(path, str):
            self.files[name] = [os.path.abspath(path)]
        else:
            self.files[name] = [os.path.abspath(p) for p in path]

    def set_files(self, name, paths_list):
        '''
        Add a file to the analysis object's 'files' dict
        name = dict key
        paths_list = list of file paths
        '''
        # self.files[name] = [os.path.abspath(path) for path in paths_list]
        self.set_file(name = name, path = paths_list)

    def add_file(self, name, path):
        '''
        Add a file to the analysis object's 'files' dict
        name = dict key
        paths_list = list of file paths
        '''
        self.files[name].append(os.path.abspath(path))


    def add_files(self, name, paths_list):
        '''
        Add a file to the analysis object's 'files' dict
        name = dict key
        paths_list = list of file paths
        '''
        for path in paths_list:
            self.files[name].append(os.path.abspath(path))

    def get_files(self, name):
        '''
        Retrieve a file by name from the object's 'files' dict
        name = dict key
        i = index entry in file list
        '''
        return(self.files[name])

    def get_dirs(self, name):
        '''
        Retrieve a file by name from the object's 'files' dict
        name = dict key
        i = index entry in file list
        '''
        return(self.dirs[name])
