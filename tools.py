#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
General utility functions and classes for the program
'''
import sys
import os
import subprocess as sp
import logging
logger = logging.getLogger("tools")
logger.debug("loading tools module")


# ~~~~ CUSTOM CLASSES ~~~~~~ #
class Container(object):
    '''
    basic container for information
    '''
    pass

class SubprocessCmd(object):
    '''
    A command to be run in subprocess
    '''
    def __init__(self, command):
        self.command = command

    def run(self, command = None):
        '''
        Run the command, capture the process object

        # universal_newlines=True required for Python 2 3 compatibility with stdout parsing
        '''
        if not command:
            command = self.command
        self.process = sp.Popen(command, stdout = sp.PIPE, shell = True, universal_newlines = True)
        self.proc_stdout = self.process.communicate()[0].strip()


class DirHop(object):
    '''
    A class for executing commands in the context of a different working directory
    adapted from: https://mklammler.wordpress.com/2011/08/14/safe-directory-hopping-with-python/

    with DirHop('/some/dir') as d:
        do_something()
    '''
    def __init__(self, directory):
        self.old_dir = os.getcwd()
        self.new_dir = directory
    def __enter__(self):
        logger.debug('Changing working directory to: {0}'.format(self.new_dir))
        os.chdir(self.new_dir)
        return(self)
    def __exit__(self, type, value, traceback):
        logger.debug('Changing working directory back to: {0}'.format(self.old_dir))
        os.chdir(self.old_dir)
        return(isinstance(value, OSError))


# ~~~~ CUSTOM FUNCTIONS ~~~~~~ #
def my_debugger(vars):
    '''
    starts interactive Python terminal at location in script
    very handy for debugging
    call this function with
    my_debugger(globals().copy())
    anywhere in the body of the script, or
    my_debugger(locals().copy())
    within a script function
    '''
    import readline # optional, will allow Up/Down/History in the console
    import code
    # vars = globals().copy() # in python "global" variables are actually module-level
    vars.update(locals())
    shell = code.InteractiveConsole(vars)
    shell.interact()

def subprocess_cmd(command, return_stdout = False):
    # run a terminal command with stdout piping enabled
    import subprocess as sp
    process = sp.Popen(command,stdout=sp.PIPE, shell=True, universal_newlines=True)
     # universal_newlines=True required for Python 2 3 compatibility with stdout parsing
    proc_stdout = process.communicate()[0].strip()
    if return_stdout == True:
        return(proc_stdout)
    elif return_stdout == False:
        logger.debug(proc_stdout)

def timestamp():
    '''
    Return a timestamp string
    '''
    import datetime
    return('{:%Y-%m-%d-%H-%M-%S}'.format(datetime.datetime.now()))

def print_dict(mydict):
    '''
    pretty printing for dict entries
    '''
    for key, value in mydict.items():
        logger.debug('{}: {}\n\n'.format(key, value))

def mkdirs(path, return_path=False):
    '''
    Make a directory, and all parent dir's in the path
    '''
    import sys
    import os
    import errno
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
    if return_path:
        return path

def write_dicts_to_csv(dict_list, output_file):
    '''
    write a list of dicts to a CSV file
    '''
    import csv
    with open(output_file, 'w') as outfile:
        fp = csv.DictWriter(outfile, dict_list[0].keys())
        fp.writeheader()
        fp.writerows(dict_list)

def backup_file(input_file, return_path=False, sys_print = False, use_logger = None):
    '''
    backup a file by moving it to a folder called 'old' and appending a timestamp
    use_logger is a logger object to log to
    '''
    import os
    if use_logger:
        logger = use_logger
    if os.path.isfile(input_file):
        filename, extension = os.path.splitext(input_file)
        new_filename = '{0}.{1}{2}'.format(filename, timestamp(), extension)
        new_filename = os.path.join(os.path.dirname(new_filename), "old", os.path.basename(new_filename))
        mkdirs(os.path.dirname(new_filename))
        logger.debug('\nBacking up old file:\n{0}\n\nTo location:\n{1}\n'.format(input_file, new_filename))
        if sys_print == True:
            logger.debug('''
To undo this, run the following command:\n
mv {0} {1}
'''.format(os.path.abspath(input_file), new_filename)
            )
        os.rename(input_file, new_filename)
    if return_path:
        return input_file

def print_json(object):
    import json
    logger.debug(json.dumps(object, sort_keys=True, indent=4))

def json_dumps(object):
    import json
    return(json.dumps(object, sort_keys=True, indent=4))


def write_json(object, output_file):
    import json
    with open(output_file,"w") as f:
        json.dump(object, f, sort_keys=True, indent=4)

def load_json(input_file):
    import json
    with open(input_file,"r") as f:
        my_item = json.load(f)
    return my_item

def item_exists(item, item_type = 'any', n = False):
    '''
    Check that an item exists
    item_type is 'any', 'file', 'dir'
    n is True or False and negates 'exists'
    '''
    exists = False
    if item_type == 'any':
        exists = os.path.exists(item)
    elif item_type == 'file':
        exists = os.path.isfile(item)
    elif item_type == 'dir':
        exists = os.path.isdir(item)
    if n:
        exists = not exists
    return(exists)

def reply_to_address(servername, username = None):
    '''
    Get the email address to use for the 'reply to' field in emails
    '''
    import getpass
    if not username:
        username = getpass.getuser()
    address = username + '@' + servername
    return(address)
