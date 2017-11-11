#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Functions & items to set up the program loggers
"""

import yaml
import logging
import logging.config
import datetime
import os


# ~~~~~ FUNCTIONS ~~~~~ #
def timestamp():
    """
    Return a timestamp string
    """
    return('{:%Y-%m-%d-%H-%M-%S}'.format(datetime.datetime.now()))

def logpath(logfile = 'log.txt'):
    """
    Return the path to the main log file; needed by the logging.yml
    use this for dynamic output log file paths & names
    """
    return(logging.FileHandler(logfile))

def log_setup(config_yaml, logger_name):
    """
    Set up the logger for the script using a YAML config file
    config = path to YAML config file
    """
    # Config file relative to this file
    loggingConf = open(config_yaml, 'r')
    logging.config.dictConfig(yaml.load(loggingConf))
    loggingConf.close()
    return(logging.getLogger(logger_name))

def logger_filepath(logger, handler_name):
    """
    Get the path to the filehander log file
    """
    log_file = None
    for h in logger.__dict__['handlers']:
        if h.__class__.__name__ == 'FileHandler':
            logname = h.get_name()
            if handler_name == logname:
                log_file = h.baseFilename
    return(log_file)

def log_all_handler_filepaths(logger):
    """
    Adds Info log messages for all filepaths for all file handlers
    """
    for h in logger.__dict__['handlers']:
        if h.__class__.__name__ == 'FileHandler':
            logname = h.get_name()
            log_file = h.baseFilename
            logger.info('Path to {0} log file: {1}'.format(logname, log_file))

def get_logger_handler(logger, handler_name, handler_type = 'FileHandler'):
    """
    Get the filehander object from a logger
    """
    for h in logger.__dict__['handlers']:
        if h.__class__.__name__ == handler_type:
            logname = h.get_name()
            if handler_name == logname:
                return(h)

def get_all_handlers(logger, types = ('FileHandler',)):
    """
    Get all logger handlers of the given types from the logger
    types = ['FileHandler', 'StreamHandler']
    x = [h for h in get_all_handlers(logger)]
    """
    for h in logger.__dict__['handlers']:
        if h.__class__.__name__ in types:
            yield(h)

def remove_handlers(logger, handlers):
    """
    Removes all the handlers from a logger
    """
    for h in logger.__dict__['handlers']:
        logger.removeHandler(h)
    return(logger)

def remove_all_handlers(logger, types = ('FileHandler', 'StreamHandler')):
    """
    Remove all of the handlers from a logger object
    """
    handlers = [h for h in get_all_handlers(logger = logger, types = types)]
    logger = remove_handlers(logger = logger, handlers = handlers)
    return(logger)

def add_handlers(logger, handlers):
    """
    Add filehandlers to the logger
    """
    # handlers = None
    if handlers == None:
        return(logger)
    # handlers is assumed to be a single handler if its not a list
    if not isinstance(handlers, list):
        logger.addHandler(handlers)
    # otherwise its assumed to be a list of handlers
    else:
        for h in handlers:
            logger.addHandler(h)
    return(logger)


def print_filehandler_filepaths_to_log(logger):
    """
    Make a log entry with the paths to each filehanlder in the logger
    """
    for h in logger.__dict__['handlers']:
        if h.__class__.__name__ == 'FileHandler':
            logname = h.get_name()
            log_file = h.baseFilename
            logger.info('"{0}" log filepath: {1}'.format(logname, log_file))

def build_logger(name, level = logging.DEBUG, log_format = '[%(asctime)s] %(levelname)s (%(name)s:%(funcName)s:%(lineno)d) %(message)s'):
    """
    Create a basic logger instance
    Only add console handler by default
    """
    # create logger instance
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # create console handler
    consolelog = logging.StreamHandler()
    consolelog.setLevel(level)
    consolelog.set_name("console")

    # create formatter and add it to the handlers
    formatter = logging.Formatter(log_format)
    formatter.datefmt = "%Y-%m-%d %H:%M:%S"
    consolelog.setFormatter(formatter)

    # add the handlers to logger
    logger.addHandler(consolelog)
    # logger.addHandler(create_main_filehandler())
    return(logger)

def create_main_filehandler(log_file, name = "main", level = logging.DEBUG, log_format = '%(asctime)s:%(name)s:%(module)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s'):
    """
    Return the 'main' file handler using globally set variables
    """
    # global scriptdir
    # global scriptname
    # global logdir
    # file_timestamp = timestamp()
    # log_file = os.path.join(scriptdir, logdir, '{0}.{1}.log'.format(scriptname, file_timestamp))
    formatter = logging.Formatter(log_format)
    formatter.datefmt = "%Y-%m-%d %H:%M:%S"
    mainhandler = logging.FileHandler(log_file)
    mainhandler.setLevel(level)
    mainhandler.set_name(name)
    mainhandler.setFormatter(formatter)
    return(mainhandler)

def email_log_filehandler(log_file, name = "emaillog", level = logging.INFO, log_format = '[%(levelname)-8s] %(message)s', datefmt = "%Y-%m-%d %H:%M:%S"):
    """
    Return a fileHandler for a log meant to be used as the body of an email
    """
    formatter = logging.Formatter(log_format)
    formatter.datefmt = datefmt
    emailhandler = logging.FileHandler(log_file)
    emailhandler.setLevel(level)
    emailhandler.set_name(name)
    emailhandler.setFormatter(formatter)
    return(emailhandler)

def log_exception(logger, errors):
    """
    Create a log entry with the errors and traceback
    """
    logger.error(errors, exc_info=True)

def has_console_handler(logger):
    """
    Searches a logger's handlers to determine if a ``console`` handler is present

    Parameters
    ----------
    logger: logging.Logger
        a ``logging.Logger`` object
    """
    # make sure there are handlers
    if len(logger.handlers) < 1:
        return(False)
    # check if one of the handlers is named 'console'
    return(any([h.name == 'console' for h in logger.handlers]))

def build_console_handler(name = "console", level = logging.DEBUG, log_format = '[%(asctime)s] %(levelname)s (%(name)s:%(funcName)s:%(lineno)d) %(message)s', datefmt = "%Y-%m-%d %H:%M:%S"):
    """
    Returns a basic "console" ``StreamHandler``
    """

    # create console handler
    consolelog = logging.StreamHandler()
    consolelog.setLevel(level)
    consolelog.set_name(name)

    # create formatter and add it to the handlers
    formatter = logging.Formatter(log_format)
    formatter.datefmt = datefmt
    consolelog.setFormatter(formatter)

    return(consolelog)


def add_missing_console_handler(logger, *args, **kwargs):
    """
    Adds a `console` ``StreamHandler`` if a handler named "console" is not present already in the logger

    Examples
    --------
    Example usage::

        >>> import log
        >>> import logging
        >>> import qsub
        >>> log.has_console_handler(qsub.logger)
        False
        >>> log.add_missing_console_handler(qsub.logger)
        >>> log.has_console_handler(qsub.logger)
        True

    """
    if not has_console_handler(logger):
        console_handler = build_console_handler(*args, **kwargs)
        logger.addHandler(console_handler)
