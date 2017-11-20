#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A collection of functions and objects for submitting jobs to the NYUMC SGE compute cluster with `qsub` from within Python, and monitoring them until completion

This submodule can also be run as a stand-alone demo script
"""
import log
import logging
logger = logging.getLogger("qsub")
logger.debug("loading qsub module")

import os
from collections import defaultdict
import subprocess as sp
import re
import time
import datetime
from time import sleep
import sys
import getpass
import json
try:
    from sh import qstat
except:
    logger.error("qstat could not be loaded")
import tools

# ~~~~ GLOBALS ~~~~~~ #
job_state_key = defaultdict(lambda: None)
"""
dictionary containing possible qsub job states; default state is None

format `key: value`, where `key` is the character string representation of the job state provided by `qstat` output, and `value` is a description of the state.

Recognized Job States
---------------------
`Eqw`: `Error`; the job is in an error status and never started running

`r`: `Running`; the job is currently running

`qw`: `Waiting`; the job is currently in the scheduler queue waiting to run

`t`: `None`; ???

`dr`: `None`; the job has been submitted for deletion and will be deleted
"""
job_state_key['Eqw'] = 'Error'
job_state_key['r'] = 'Running'
job_state_key['qw'] = 'Waiting'
job_state_key['t'] = None
job_state_key['dr'] = None


# ~~~~ CUSTOM CLASSES ~~~~~~ #
class Job(object):
    """
    Main object class for tracking and validating a compute job that has been submitted to the HPC cluster with the `qsub` command

    Notes
    -----
    The default action upon initialization is to query `qstat` to determine whether the job is currently running. After a job has completed, built-in methods can be used to query `qacct -j` to determine if the job finished with a successful exit status. Both `qstat` and `qacct` are queried by making system calls to the the corresponding programs and parsing their stdout messages.

    Many of the methods included with this object class have stand-alone functions of the same name, with the same usage & functionality.

    Examples
    ----------
    Example usage::

        x = qsub.Job('2379768')
        x.running()
        x.present()
    """
    def __init__(self, id, name = None, log_dir = None, debug = False):
        """
        Parameters
        ----------
        id: int
            numeric job ID, as returned by `qsub` at job submission
        name: str
            the name given to the compute job
        log_dir: str
            path to the directory used to hold log output by the compute job
        debug: bool
            intialize the job without immediately querying `qstat` to determine job status

        Attributes
        ----------
        job_state_key: dict
            the module's `job_state_key` object
        id: int
            a numeric ID for the Job object
        name: str
            a name for the Job
        log_dir: str
            path to the directory used to hold log output by the compute job
        log_paths: dict
            dictionary containing the types and paths to the job's output logs
        completions: str
            character string used to describe the job and its completion states
        """
        global job_state_key
        self.job_state_key = job_state_key
        self.id = id
        self.name = name
        self.log_dir = log_dir
        self.log_paths = {}
        if log_dir:
            self.update_log_files()
        # hold a character string of completion validation information
        self.completions = self._completions()

        # add the rest of the attributes as per the update function
        if not debug:
            self._update()
    def __repr__(self):
        return('Job(id = {0}, name = {1}, log_dir = {2})'.format(self.id, self.name, self.log_dir))

    # ~~~~~ Methods for determining running job state from qstat ~~~~~ #
    def _completions(self):
        """
        Makes a default 'completions' string attribute

        Returns
        -------
        str
            character string describing the object and its qsub log paths
        """
        return('{0}\nlog_paths = {1}\n'.format(self.__repr__(), self.log_paths))

    def update_log_files(self, _type = 'stdout'):
        """
        Updates the paths to the log files in the `log_paths` attribute
        """
        log_path = self.get_log_file(_type = _type)
        self.log_paths.update({_type: log_path})

    def get_log_file(self, _type = 'stdout'):
        """
        Returns the expected path to the job's log file

        Parameters
        ----------
        _type: str
            either 'stdout' or 'stderr', representing the type of log path to generate

        Notes
        -----
        A stdout log file basename for a compute job with an ID of `4088513` and a name of `python` would look like this: `python.o4088513`
        The corresponding stderr log name would look like: `python.e4088513`
        """
        if not self.log_dir:
            logger.warning('log_dir attribute is not set for this qsub job: {0}'.format((self.id, self.name)))
            return(None)
        type_key = {'stdout': '.o', 'stderr': '.e'}
        type_char = type_key[_type]
        logfile = str(self.name) + type_char + str(self.id)
        log_path = os.path.join(str(self.log_dir), logfile)
        if not tools.item_exists(log_path):
            logger.warning('Log file does not appear to exist: {0}'.format(log_path))
        return(log_path)


    def get_job(self, id, qstat_stdout = None):
        """
        Retrieves the job's `qstat` entry

        Returns
        -------
        str
        """
        import re
        try:
            from sh import qstat
        except:
            logger.error("qstat could not be loaded")
        job_id_pattern = r"^\s*{0}\s.*$".format(id)
        if not qstat_stdout:
            qstat_stdout = qstat()
        entry = re.findall(str(job_id_pattern), str(qstat_stdout), re.MULTILINE)
        return(entry)

    def get_status(self, id, entry = None, qstat_stdout = None):
        """
        Gets the status of the qsub job, e.g. "Eqw", "r", etc.

        Returns
        -------
        str
        """
        import re
        # regex for the pattern matching https://docs.python.org/2/library/re.html
        job_id_pattern = r"^.*\s*{0}.*\s([a-zA-Z]+)\s.*$".format(id)
        if not entry:
            entry = self.get_job(id = id, qstat_stdout = qstat_stdout)
        status = re.search(str(job_id_pattern), str(entry), re.MULTILINE)
        if status:
            return(status.group(1))
        else:
            return(status)

    def get_state(self, status, job_state_key):
        """
        Gets the interpretation of the job's status from the `job_state_key`, e.g. "Running", etc.

        Returns
        -------
        str
        """
        # defaultdict returns None if the key is not present
        state = job_state_key[str(status)]
        return(state)

    def get_is_running(self, state, job_state_key):
        """
        Checks if the job is considered to be running

        Returns
        -------
        bool
        """
        is_running = False
        if state in ['Running']:
            is_running = True
        return(is_running)

    def get_is_error(self, state, job_state_key):
        """
        Checks if the job is considered to in an error state

        Returns
        -------
        bool
        """
        is_running = False
        if state in ['Error']:
            is_running = True
        return(is_running)

    def get_is_present(self, id, entry = None, qstat_stdout = None):
        """
        Finds out if a job is present in qsub

        Returns
        -------
        bool
        """
        if not entry:
            entry = self.get_job(id = id, qstat_stdout = qstat_stdout)
        if entry:
            return(True)
        else:
            return(False)

    def _update(self):
        """
        Update the object's status attributes based on `qstat` stdout messages
        """
        self.qstat_stdout = qstat()
        self.entry = self.get_job(id = self.id, qstat_stdout = self.qstat_stdout)
        self.status = self.get_status(id = self.id, entry = self.entry, qstat_stdout = self.qstat_stdout)
        self.state = self.get_state(status = self.status, job_state_key = self.job_state_key)
        self.is_running = self.get_is_running(state = self.state, job_state_key = self.job_state_key)
        self.is_error = self.get_is_error(state = self.state, job_state_key = self.job_state_key)
        self.is_present = self.get_is_present(id = self.id, entry = self.entry, qstat_stdout = self.qstat_stdout)

    def _debug_update(self, qstat_stdout):
        """
        Debug update mode with requires a qstat_stdout to be passed manually after object initialization
        """
        self.qstat_stdout = qstat_stdout
        self.entry = self.get_job(id = self.id, qstat_stdout = self.qstat_stdout)
        self.status = self.get_status(id = self.id, entry = self.entry, qstat_stdout = self.qstat_stdout)
        self.state = self.get_state(status = self.status, job_state_key = self.job_state_key)
        self.is_running = self.get_is_running(state = self.state, job_state_key = self.job_state_key)
        self.is_present = self.get_is_present(id = self.id, entry = self.entry, qstat_stdout = self.qstat_stdout)

    def running(self):
        """
        Returns `True` or `False` whether or not the job is currently considered to be running

        Returns
        -------
        bool
            `True` if running, otherwise `False`
        """
        self._update()
        return(self.is_running)

    def error(self):
        """
        Returns `True` or `False`  whether or not the job is currently considered to be in an error state

        Returns
        -------
        bool
            `True` if in error, otherwise `False`
        """
        self._update()
        return(self.is_error)

    def present(self):
        """
        Returns `True` or `False`  whether or not the job is currently in the `qstat` queue

        Returns
        -------
        bool
            `True` if present, otherwise `False`
        """
        self._update()
        return(self.is_present)

    # ~~~~~ Methods for querying qacct for job completion status ~~~~~ #
    def get_qacct(self, job_id = None):
        """
        Gets the `qacct` entry for a completed qsub job, used to determine if the job completed successfully

        Notes
        -----
        This operation is extremely slow, takes about 10 - 30+ seconds to complete

        Returns
        -------
        str
            The character string representation of the stdout from the `qacct -j` command for the job
        """
        if not job_id:
            job_id = self.id
        qacct_command = 'qacct -j {0}'.format(job_id)
        run_cmd = tools.SubprocessCmd(command = qacct_command).run()
        return(run_cmd.proc_stdout)

    def qacct2dict(self, proc_stdout = None, entry_delim = None):
        """
        Converts text output from `qacct` into a dictionary for parsing

        Parameters
        ----------
        entry_delim: str
            character string delimiter to split entries in the `qacct` output, defaults to '=============================================================='

        Returns
        -------
        dict
            a dictionary of individual records containing metadata about the completion status of jobs with the matching `job_id`

        Notes
        -----
        `qacct` returns multiple entries per `job_id`, because the `job_id` wrap around. So multiple historic jobs with the same `job_id` number will also be returned, delimited by a long string of `===`

        """
        if not proc_stdout:
            proc_stdout = self.get_qacct()
        # entries for jobs in the qacct stdout are split with this character string
        if not entry_delim:
            entry_delim = '=============================================================='
        # dict to hold the items in each entry
        entry_dict = {}
        # split the large blob of stdout text into separate job entries; skip empty entries
        entries = [c for c in proc_stdout.split(entry_delim) if c != '']
        # each entry has one item per line, separated by a variable amount of whitespace
        for i, entry in enumerate(entries):
            # items in the entry
            entry_stats = entry.split('\n')
            entry_dict[i] = {}
            for item in entry_stats:
                # split on the first whitespace; make sure at least 2 entries are produced
                if len(item.split(None, 1)) >=2:
                    key, value = item.split(None, 1)
                    entry_dict[i][key] = value.strip()
        return(entry_dict)

    def filter_qacct(self, qacct_dict = None, days_limit = 7, username = None):
        """
        Filters out 'bad' entries from the `qacct` output dictionary

        Parameters
        ----------
        qacct_dict: dict
            dictionary containing job records which represent `qacct` entries
        days_limit: int or None
            Maximum allowed age of a job. Defaults to 7 days, change this to `None` to disable date filtering
        username: str
            The username which `qacct` records must match, defaults to the current user's name

        Returns
        -------
        dict
            a dictionary which will hopefully contain only one `qacct` record, hopefully matching the intended compute job

        Notes
        -----
        Filtering is required to remove historic job records from the `qacct` output; only one record can remain in order for the job's completeion status to be determined. This function will try to identify entries which are extraneous and do not represent the intended compute job. The default filtering criteria will first try filter out records that contain usernames which do not match that of the current user. Next, records with a timestamp older than the provided `days_limit` will also be filtered out, in case the current user has multiple job entries for the given `job_id`. Note that the timestamp format used in the `qacct` output is inconsistent, so this type of filtering may be prone to errors.

        Todo
        ----
        Come up with other criteria for filtering out unwanted job entries here...

        """
        if not username:
            username = getpass.getuser()

        if not qacct_dict:
            qacct_dict = self.qacct2dict()

        for key, subdict in qacct_dict.items():
            # only keep the entries that match the current user's username
            job_owner = subdict['owner']
            if job_owner != username:
                qacct_dict.pop(key)
                continue

            # make sure the job was completed within the last 7 days
            job_end_time = subdict['end_time']
            # TODO: sometimes the timestamp is not in the correct format, need to have try/except here
            job_end_time_obj = datetime.datetime.strptime(job_end_time, "%c")
            time_now = datetime.datetime.now()
            time_elapsed = time_now - job_end_time_obj
            if days_limit:
                if time_elapsed.days > days_limit:
                    qacct_dict.pop(key)
                    continue
        # more filter criteria here...
        return(qacct_dict)

    def get_qacct_job_failed_status(self, failed_entry):
        """
        Special parsing for the 'failed' entry in qacct output
        because its not a plain digit value its got some weird text description stuck in there too

        Returns
        -------
        int
            the first int value found after splitting text on the first whitespace found

        Examples
        --------
        Example of weird 'failed' entry that needs to be parsed::

            {'failed': '100 : assumedly after job'}

        In this case, the value 100 would be returned

        """
        # get the first entry in the line split by whitespace
        value = failed_entry.split(None, 1)[0]
        value = int(value)
        return(value)

    def update_completion_validations(self, validation_dict):
        """
        Updates the `completion_validations` dict of validation stats with a pretty printed view of the `validations` dictionary, along with the Job's text string representation
        """

        self.completion_validations.update(validation_dict)
        self.validations = json.dumps(self.completion_validations, indent = 4)
        # self.completions = '{0}\n{1}\n'.format(self.__repr__(), self.validations)
        self.completions = self._completions() + self.validations


    def validate_completion(self, job_id = None, *args, **kwargs):
        """
        Checks if the qsub job completed successfully. Multiple validation criteria are evaluated one at a time, and the results of each are added to a `completion_validations` dictionary attribute along with a verbose description of the criteria. After all the criteria have been evaluated, returns a boolean `True` or `False` to determine if all criteria passed validation. This determines if a compute job is considered to have completed successfully or not.

        Returns
        -------
        bool
            `True` or `False`, whether or not all job completion validation criteria passed
        """
        if not job_id:
            job_id = self.id

        # create a list of validations for the object
        self.completion_validations = {}

        # make sure the job is not currently running or in queues
        validation = {
            'qtat_presence': {
            'status': self.present(),
            'note': None
            }
        }
        validation = {'qtat_presence': {'status': self.present()}}
        if validation['qtat_presence']['status']:
            validation['qtat_presence']['note'] = 'The job is still present in qstat and has not completed yet; job cannot be validated'
            self.update_completion_validations(validation)
            # logger.error('Job {0} is still running and cannot be validated for completion'.format(job_id))
            return(False)
        else:
            validation['qtat_presence']['note'] = 'The job is not present in qstat and has completed'
            self.update_completion_validations(validation)

        # get the results of the qacct query command
        if not hasattr(self, 'qacct_stdout'):
            # allow qacct_stdout to be pre-set externally for debugging & testing
            # also prevent querying qacct multiple times for the same job
            self.qacct_stdout = self.get_qacct(job_id = job_id)

        # convert it into a dict
        self.qacct_dict = self.qacct2dict(proc_stdout = self.qacct_stdout)
        # filter extraneous entries
        self.qacct_dict = self.filter_qacct(qacct_dict = self.qacct_dict, *args, **kwargs)

        # make sure there are entries left
        validation = {
            'has_qacct_entries': {
            'status': None,
            'note': None
            }
        }
        if not self.qacct_dict:
            validation['has_qacct_entries']['status'] = False
            validation['has_qacct_entries']['note'] = 'No entries were left in qacct job record output after filtering; job cannot be validated'
            self.update_completion_validations(validation)
            # logger.error('No valid job entries found for job_id {0}'.format(job_id))
            return(False)
        else:
            validation['has_qacct_entries']['status'] = True
            validation['has_qacct_entries']['note'] = 'At least one entry was left in qacct job record output after filtering'
            self.update_completion_validations(validation)

        # make sure only one entry is left!
        validation = {
            'has_only_one_qacct_entry': {
            'status': None,
            'note': None
            }
        }
        if len(self.qacct_dict.keys()) > 1:
            validation['has_only_one_qacct_entry']['status'] = False
            validation['has_only_one_qacct_entry']['note'] = 'More than one entry was left in qacct job record output after filtering; job cannot be validated'
            self.update_completion_validations(validation)
            # logger.debug('Multiple entries found for job_id {0};\n{1}'.format(job_id, qacct_dict))
            return(False)
        else:
            validation['has_only_one_qacct_entry']['status'] = True
            validation['has_only_one_qacct_entry']['note'] = 'Only one entry was left in qacct job record output after filtering'
            self.update_completion_validations(validation)

        # example qacct output:
        # failed       0
        # exit_status  1

        # check the 'failed' status; >0 = failed !!
        validation = {
            'failed_status_0': {
            'status': False,
            'note': None
            }
        }
        # get the key index for the first entry inthe dict
        first_entry_key = self.qacct_dict.keys()[0]
        status_code = self.get_qacct_job_failed_status(failed_entry = self.qacct_dict[first_entry_key]['failed'])
        if status_code > 0:
            validation['failed_status_0']['status'] = False
            validation['failed_status_0']['note'] = 'The "failed" qacct value for the job was {0}; >0 means the job failed'.format(status_code)
            self.update_completion_validations(validation)
        else:
            validation['failed_status_0']['status'] = True
            validation['failed_status_0']['note'] = 'The "failed" qacct value for the job was {0}; >0 means the job failed'.format(status_code)
            self.update_completion_validations(validation)

        # check the 'exit_status'
        validation = {
            'exit_status_0': {
            'status': False,
            'note': None
            }
        }
        first_entry_key = self.qacct_dict.keys()[0]
        exit_status = int(self.qacct_dict[first_entry_key]['exit_status'])
        if exit_status > 0:
            validation['exit_status_0']['status'] = False
            validation['exit_status_0']['note'] = 'The "exit_status" qacct value for the job was {0}; >0 means the job failed'.format(exit_status)
            self.update_completion_validations(validation)
        else:
            validation['exit_status_0']['status'] = True
            validation['exit_status_0']['note'] = 'The "exit_status" qacct value for the job was {0}; >0 means the job failed'.format(exit_status)
            self.update_completion_validations(validation)
        # add more criteria here...

        # aggregate the validations
        validations = [
        self.completion_validations['exit_status_0']['status'],
        self.completion_validations['failed_status_0']['status']
        ]

        # check if not all validations are True...
        if not all(validations):
            # logger.error('The job {0} is not valid'.format(job_id))
            # logger.error({'validate_failed_status': validate_failed_status, 'validate_exit_status': validate_exit_status})
            return(False)
        else:
            # logger.debug('The job {0} is valid'.format(job_id))
            return(True)





# ~~~~~~ JOB FUNCTIONS ~~~~~ #
def submit(verbose = False, log_dir = None, monitor = False, validate = False, *args, **kwargs):
    """
    Submits a shell command to be run as a `qsub` compute job. Returns a `Job` object. Passes args and kwargs to `submit_job`. Compute jobs are created by assembling a `qsub` shell command using a bash heredoc wrapped around the provided shell command to be executed. The numeric job ID and job name echoed by `qsub` on stdout will be captured and used to generate a 'Job' object.

    Parameters
    ----------
    verbose: bool
        `True` or `False`, whether or not the generated `qsub` command should be printed in log output
    log_dir: str
        the directory to use for qsub job log output files, defaults to the current working directory
    monitor: bool
        whether the job should be immediately monitored until completion
    validate: bool
        whether or not the job should immediately be validated upon completion
    *args: list
        list of arguments to pass on to `submit_job`
    **kwargs: dict
        dictionary of args to pass on to `submit_job`

    Returns
    -------
    Job
        a `Job` object, representing a `qsub` compute job that has been submitted to the HPC cluster

    Examples
    --------
    Example usage::

        job = submit(command = 'echo foo')
        job = submit(command = 'echo foo', log_dir = "logs", print_verbose = True, monitor = True, validate = True)

    """
    # check if log_dir was passed
    if log_dir:
        # create the dir if it doesnt exist already
        tools.mkdirs(log_dir)
        # only continue if the log_dir exists now
        if not tools.item_exists(item = log_dir, item_type = 'dir'):
            logger.warning('log_dir does not exist and will not be used for qsub job submission; {0}'.format(log_dir))
        else:
            # resolve the path to the full, expanded, absolute, real path - bad log_dir paths break job submissions easily
            log_dir = os.path.realpath(os.path.expanduser(log_dir))
            stdout_log_dir = log_dir
            stderr_log_dir = log_dir
            kwargs.update({
                'stdout_log_dir': stdout_log_dir,
                'stderr_log_dir': stderr_log_dir
                })

    proc_stdout = submit_job(return_stdout = True, verbose = verbose, *args, **kwargs)
    job_id, job_name = get_job_ID_name(proc_stdout)
    job = Job(id = job_id, name = job_name, log_dir = log_dir)

    # optionally, monitor the job to completion
    if monitor:
        monitor_jobs(jobs = [job], **kwargs)
    # optionally, validate the job completion
    if validate:
        job.validate_completion()
    return(job)


def subprocess_cmd(command, return_stdout = False):
    """
    Runs a terminal command with stdout piping enabled

    Notes
    -----
    `universal_newlines=True` required for Python 2 3 compatibility with stdout parsing

    """
    process = sp.Popen(command,stdout=sp.PIPE, shell=True, universal_newlines=True)
    proc_stdout = process.communicate()[0].strip()
    if return_stdout == True:
        return(proc_stdout)
    elif return_stdout == False:
        logger.debug(proc_stdout)


def get_job_ID_name(proc_stdout):
    """
    Parses stdout text to find lines that match the output message from a `qsub` job submission

    Returns
    -------
    tuple
        `(<job id number>, <job name>)`

    Examples
    --------
    Example usage::

        proc_stdout = submit_job(return_stdout = True) # 'Your job 1245023 ("python") has been submitted'
        job_id, job_name = get_job_ID_name(proc_stdout)

    """
    proc_stdout_list = proc_stdout.split()
    job_id = proc_stdout_list[2]
    job_name = proc_stdout_list[3]
    job_name = re.sub(r'^\("', '', str(job_name))
    job_name = re.sub(r'"\)$', '', str(job_name))
    return((job_id, job_name))


def submit_job(command = 'echo foo', params = '-j y', name = "python", stdout_log_dir = None, stderr_log_dir = None, return_stdout = False, verbose = False, pre_commands = 'set -x', post_commands = 'set +x', sleeps = 0.5, print_verbose = False, **kwargs):
    """
    Internal function for submitting compute jobs to the HPC cluster running SGE by using the `qsub` shell command. Call this function with `submit` instead; args and kwargs will be evaluated here. Creates a `qsub` shell command to be run in a subprocess, submitting the cluster job with a bash heredoc wrapper.
    Basic format for job submission to the SGE cluster with qsub
    using a bash heredoc format

    Parameters
    ----------
    command: str
        shell commands to be run inside the compute job
    params: str
        extra params to be passed to `qsub`
    name: str
        the name of the qsub compute job
    stdout_log_dir: str
        the path to the directory to use for `qsub` log output; if `None`, defaults to the current working directory
    stderr_log_dir: str
        the path to the directory to use for `qsub` log output; if `None`, defaults to the current working directory
    return_stdout: bool
        whether or not the function should `return` the stdout of the `qsub` submission subprocess call, its recommened to always leave this set to `True`, otherwise stdout will be printed to program the log output
    verbose: bool
        whether or not the generated `qsub` command should be printed in program log output
    pre_commands: str
        commands to run before the `command` inside the qsub job; defaults to 'set -x' in order to provide verbose qsub log output, you can also put environment modulation code here.
    post_commands: str
        commands to run after the `command` inside the qsub job; defaults to 'set +x'
    sleeps: int
        number of seconds to `sleep` after submitting a `qsub` job; it is recommened to leave this set to a value >0 in order to avoid overwhelming the job scheduler with requests
    print_verbose: bool
        print the generated `qsub` command to the console with the Python `print` function (as opposed to logger output)

    Returns
    -------
    str
        returns the stdout of the evaluated `qsub` shell command, assuming `return_stdout = True` was passed. Otherwise, returns nothing.

    Notes
    -----
    `stdout_log_dir` and `stderr_log_dir` should have trailing slashes in their paths, and are set to the same path by default using the `log_dir` arg in `submit`

    Malformed or nonexistant `stdout_log_dir` and `stderr_log_dir` paths are a common source for compute job failure.

    Call this function with `submit` instead.

    This function generates a `qsub` shell command in a format such as this::

        qsub -j y -N "python" -o :"/ifs/data/molecpathlab/scripts/snsxt/snsxt/util/" -e :"/ifs/data/molecpathlab/scripts/snsxt/snsxt/util/" <<E0F
        set -x

            cat /etc/hosts
            sleep 10

        set +x
        E0F

    The generated shell command will be evaluated by Python `subprocess`, and its stdout messages returned.

    """
    if not stdout_log_dir:
        stdout_log_dir = os.path.join(os.getcwd(), '')
    if not stderr_log_dir:
        stderr_log_dir = os.path.join(os.getcwd(), '')
    qsub_command = """
qsub {0} -N "{1}" -o :"{2}" -e :"{3}" <<E0F
{4}
{5}
{6}
E0F
""".format(
params,  # 0
name, # 1
stdout_log_dir, # 2
stderr_log_dir, # 3
pre_commands, # 4
command, # 5
post_commands # 6
)
    if verbose == True:
        logger.debug('qsub command is:\n{0}'.format(qsub_command))

    if print_verbose:
        print('qsub command is:\n{0}'.format(qsub_command))

    # submit the job
    proc_stdout = subprocess_cmd(command = qsub_command, return_stdout = True)

    # sleep after submitting the job
    if sleeps:
        sleep(sleeps)
    if return_stdout == True:
        return(proc_stdout)
    elif return_stdout == False:
        logger.debug(proc_stdout)

def monitor_jobs(jobs = None, kill_err = True, print_verbose = False, **kwargs):
    """
    Monitors a list of qsub `Job` objects for completion. Job monitoring is accomplished by calling each job's `present()` and `error()` methods, then waiting for several seconds. Jobs that are no longer present in `qstat` or have an error state will be removed from the monitoring queue. The function will repeatedly check each job and then wait, removing absent or errored jobs, until no jobs remain in the monitoring queue. Optionally, jobs that had an error status will be killed with the `qdel` command, or else they will remain in `qstat` indefinitely.

    This function allows your program to wait for jobs to finish running before continuing.

    Parameters
    ----------
    jobs: list
        a list of `Job` objects
    kill_err: bool
        `True` or `False`, whether or not jobs left in error state should be automatically killed. Its recommened to leave this `True`
    print_verbose: bool
        whether or not descriptions of the steps being taken should be printed to the console with Python's `print` function

    Returns
    -------
    tuple
        a tuple of lists containing `Job` objects, in the format: `(completed_jobs, err_jobs)`

    Notes
    -----
    This function will only check whether a job is present/absent in the `qstat` queue, or in an error state in the `qstat` queue; it does not actually check if a job is in a 'Running' state.

    If a job is present and not in error state, it is assumed to either be 'qw' (waiting to run), or 'r' (running). In both cases, it is assumed that the job will eventually finish and leave the `qstat` queue, and subsequently be removed from this function's monitoring queue.

    Jobs in 'Eqw' error state are stuck and will not leave on their own so must be removed automatically by this function, or killed manually by the end user.

    The ``jobs`` is mutable and passed by reference; this means that upon completion of this function, the original ``jobs`` list will be depleted::

        >>> import qsub
        >>> jobs = []
        >>> len(jobs)
        0
        >>> for i in range(5):
        ...     job = qsub.submit('sleep 20')
        ...     jobs.append(job)
        ...
        >>> len(jobs)
        5
        >>> qsub.monitor_jobs(jobs = jobs)
        ([Job(id = 4098911, name = python, log_dir = None), Job(id = 4098913, name = python, log_dir = None), Job(id = 4098915, name = python, log_dir = None), Job(id = 4098912, name = python, log_dir = None), Job(id = 4098914, name = python, log_dir = None)], [])
        >>> len(jobs)
        0

    Examples
    --------
    Example usage::

        job = submit(print_verbose = True)
        completed_jobs, err_jobs = monitor_jobs([job], print_verbose = True)
        [job.validate_completion() for job in completed_jobs]

    """
    # make sure jobs were passed
    if not jobs or len(jobs) < 1:
        logger.error('No jobs to monitor')
        return()
    # make sure jobs is a list
    if not isinstance(jobs, list):
        logger.error('"jobs" passed is not a list')
        return()

    completed_jobs = []
    # jobs in error state; won't finish
    err_jobs = []
    num_jobs = len(jobs)
    logger.debug('Monitoring jobs for completion. Number of jobs in queue: {0}'.format(num_jobs))
    if print_verbose: print('Monitoring jobs for completion. Number of jobs in queue: {0}'.format(num_jobs))
    while num_jobs > 0:
        # check number of jobs in the list
        if num_jobs != len(jobs):
            num_jobs = len(jobs)
            logger.debug("Number of jobs in queue: {0}".format(num_jobs))
            if print_verbose: print("Number of jobs in queue: {0}".format(num_jobs))
        # check each job for presence & error state
        for i, job in enumerate(jobs):
            if not job.present():
                completed_jobs.append(jobs.pop(i)) # jobs.remove(job)
            if job.error():
                err_jobs.append(jobs.pop(i))
        sleep(5)
    logger.debug('No jobs remaining in the job queue')
    if print_verbose: print('No jobs remaining in the job queue')

    # check if there were any jobs left in error state
    if err_jobs:
        logger.error('{0} jobs left were left in error state. Jobs: {1}'.format(len(err_jobs), [job.id for job in err_jobs]))
        if print_verbose: print('{0} jobs left were left in error state. Jobs: {1}'.format(len(err_jobs), [job.id for job in err_jobs]))
        # kill the error jobs with the 'qdel' command
        if kill_err:
            logger.debug('Killing jobs left in error state')
            if print_verbose: print('Killing jobs left in error state')
            qdel_command = 'qdel {0}'.format(' '.join([job.id for job in err_jobs]))
            cmd = tools.SubprocessCmd(command = qdel_command).run()
            logger.debug(cmd.proc_stdout)
            if print_verbose: print(cmd.proc_stdout)
    return((completed_jobs, err_jobs))

def kill_jobs(jobs):
    """
    Kills qsub jobs by issuing the ``qdel`` command

    Parameters
    ----------
    jobs: list
        a list of ``Job`` objects
    """
    if jobs:
        logger.debug('Killing jobs: {0}'.format(jobs))
        qdel_command = 'qdel {0}'.format(' '.join([job.id for job in jobs]))
        cmd = tools.SubprocessCmd(command = qdel_command).run()
        logger.debug(cmd.proc_stdout)
        logger.debug(cmd.proc_stderr)
    else:
        logger.debug("No jobs passed")

def kill_job_ids(job_ids):
    """
    Kills qsub jobs by issuing the ``qdel`` command

    Parameters
    ----------
    job_ids: list
        a list of job ID numbers

    Examples
    --------
    Example usage::

        import qsub
        job_ids = ['4104004', '4104006', '4104009']
        qsub.kill_job_ids(job_ids = job_ids)

    """
    if job_ids:
        logger.debug('Killing jobs: {0}'.format(job_ids))
        qdel_command = 'qdel {0}'.format(' '.join([job_id for job_id in job_ids]))
        cmd = tools.SubprocessCmd(command = qdel_command).run()
        logger.debug(cmd.proc_stdout)
        logger.debug(cmd.proc_stderr)
    else:
        logger.debug("No jobs passed")

def find_all_job_id_names(text):
    """
    Searchs a multi-line character string for all `qsub` job submission messages, where `text` represents the stdout from a series of shell commands where are assumed to have submitted a number of `qsub` jobs (e.g. by an external program)

    Parameters
    ----------
    text: str
        a single character string, e.g. representing line(s) of text assumed to be stdout from a shell command that submitted `qsub` jobs

    Notes
    -----
    This function works by parsing the provided text for lines that look like this::

        Your job 3947957 ("sns.wes.SeraCare-1to1-Positive") has been submitted

    Examples
    --------
    Example usage::

        >>> text = '\\n\\n process sample SeraCare-1to1-Positive\\n\\n CMD: qsub -q all.q -cwd -b y -j y -N sns.wes.SeraCare-1to1-Positive -M kellys04@nyumc.org -m a -hard -l mem_free=64G -pe threaded 8-16 bash /ifs/data/molecpathlab/scripts/snsxt/sns_output/test/sns/routes/wes.sh /ifs/data/molecpathlab/scripts/snsxt/sns_output/test SeraCare-1to1-Positive\\nYour job 3947957 ("sns.wes.SeraCare-1to1-Positive") has been submitted\\n\\n'
        >>> [(job_id, job_name) for job_id, job_name in find_all_job_id_names(text)]
        [('3947957', 'sns.wes.SeraCare-1to1-Positive')]

    """
    # split the lines
    text_lines = text.split('\n')
    for line in text_lines:
        # Your job 3947957 ("sns.wes.SeraCare-1to1-Positive") has been submitted
        parts = line.split()
        # ['Your', 'job', '3947957', '("sns.wes.SeraCare-1to1-Positive")', 'has', 'been', 'submitted']
        if len(parts) >= 7:
            if parts[0] == 'Your' and parts[1] == 'job' and parts[4] == 'has' and parts[5] == 'been' and parts[6] == 'submitted':
                job_id, job_name = get_job_ID_name(proc_stdout = line)
                # job = Job(id = job_id, name = job_name)
                yield(job_id, job_name)


# ~~~~~~ COMPLETED JOB VALIDATION ~~~~~ #
def get_qacct(job_id):
    """
    Gets the qacct entry for a completed qsub job
    """
    qacct_command = 'qacct -j {0}'.format(job_id)
    run_cmd = tools.SubprocessCmd(command = qacct_command).run()
    return(run_cmd.proc_stdout)

def qacct2dict(proc_stdout):
    """
    Converts text output from qacct into a dictionary for parsing
    """
    entry_dict = {}
    entry_delim = '=============================================================='
    entries = [c for c in proc_stdout.split(entry_delim) if c != '']
    for i, entry in enumerate(entries):
        entry_stats = entry.split('\n')
        entry_dict[i] = {}
        for item in entry_stats:
            if len(item.split(None, 1)) >=2:
                key, value = item.split(None, 1)
                entry_dict[i][key] = value.strip()
    return(entry_dict)


def filter_qacct(qacct_dict, days_limit = 7):
    """
    Filters out 'bad' entries from the dict
    """
    username = getpass.getuser()
    if qacct_dict:
        for key, subdict in qacct_dict.items():
            # only keep the entries that match the current user's username
            job_owner = subdict['owner']
            if job_owner != username:
                qacct_dict.pop(key)
                continue
            # make sure the job was completed within the last 7 days
            job_end_time = subdict['end_time']
            job_end_time_obj = datetime.datetime.strptime(job_end_time, "%c")
            time_now = datetime.datetime.now()
            time_elapsed = time_now - job_end_time_obj
            if time_elapsed.days > days_limit:
                qacct_dict.pop(key)
                continue
    # more filter criteria here...
    return(qacct_dict)

def get_qacct_job_failed_status(failed_entry):
    """
    Special parsing for the 'failed' entry in qacct output
    because its not a plain digit value its got some weird text description stuck in there too sometimes

    Examples
    --------
    Example text that needs parsing::

        {'failed': '100 : assumedly after job'}
    """
    # get the first entry in the line split by whitespace
    value = failed_entry.split(None, 1)[0]
    value = int(value)
    return(value)

def validate_job_completion(job_id):
    """
    Checks if a qsub job completed successfully
    """
    # get the results of the qacct query command
    proc_stdout = get_qacct(job_id = job_id)
    # convert it into a dict
    qacct_dict = qacct2dict(proc_stdout = proc_stdout)
    # filter extraneous entries
    qacct_dict = filter_qacct(qacct_dict = qacct_dict)
    # make sure there are entrie left
    if not qacct_dict:
        print('ERROR: no valid job entries found for job_id {0}'.format(job_id))
        return()
    # make sure only one entry is left!
    if len(qacct_dict.keys()) > 1:
        print('ERROR: multiple entries found for job_id {0};\n{1}'.format(job_id, qacct_dict))
        return()
    # check the 'failed' status; >0 = failed !!
    validate_failed_status = True
    first_entry_key = qacct_dict.keys()[0]
    status_code = get_qacct_job_failed_status(failed_entry = qacct_dict[first_entry_key]['failed'])
    if status_code > 0:
        validate_failed_status = False
    # check the 'exit_status'
    validate_exit_status = True
    first_entry_key = qacct_dict.keys()[0]
    if int(qacct_dict[first_entry_key]['exit_status']) > 0:
        validate_exit_status = False
    # add more criteria here...
    # aggregate the validations
    validations = [
    validate_failed_status,
    validate_exit_status
    ]
    # check if not all validations are True...
    if not all(validations):
        print('ERROR: The job {0} is not valid'.format(job_id))
        print({'validate_failed_status': validate_failed_status, 'validate_exit_status': validate_exit_status})
        return(False)
    else:
        print('The job {0} is valid'.format(job_id))
        return(True)


# ~~~~~~ DEMO FUNCTIONS ~~~~~ #
def demo_qsub():
    """
    Demo the qsub code functions

    Examples
    --------
    Example usages::

        import qsub; job = qsub.submit(log_dir = "logs", print_verbose = True); qsub.monitor_jobs([job], print_verbose = True); job.validate_completion(); print(job.completions)

        import qsub; job = qsub.submit(log_dir = "logs", print_verbose = True, monitor = True); job.validate_completion()

        import qsub; job = qsub.submit(log_dir = "logs", print_verbose = True, monitor = True, validate = True)
    """
    print('running single-job demo')

    command = """
    set -x
    cat /etc/hosts
    sleep 10
    """

    job = submit(command = command, print_verbose = True)
    print('job id: {0}'.format(job.id))
    print('job name: {0}'.format(job.name))

    print('waiting on job to finish')
    monitor_jobs(jobs = [job])
    print('job has finished\n\n')

    print('validating job completeion...')
    if job.validate_completion():
        print('Job completed successfully')
    else:
        print('Job did not pass completion validation!!!')

    return()

def demo_multi_qsub(job_num = 3):
    """
    Demo of the qsub code functions. Submits multiple jobs and monitors them to completion.
    """
    job_num = int(job_num)

    print('running multi-job demo')

    command = """
    set -x
    cat /etc/hosts
    sleep 10
    sleep $((1 + RANDOM % 10))
    """

    # list to capture the jobs as they are created
    jobs = []

    # submit a number of jobs
    for i in range(job_num):
        job = submit(command = command, print_verbose = True)
        print('submitted job: {0} "{1}"'.format(job.id, job.name))
        jobs.append(job)

    # wait for jobs to finish
    print('waiting on jobs to finish')
    monitor_jobs(jobs = jobs)
    print('jobs have finished\n\n')

    print('validating job completions...')

    if all([job.validate_completion() for job in jobs]):
        print('All jobs completed successfully')
    else:
        for job in jobs:
            if not job.validate_completion():
                print('Job {0} did not complete successfully!'.format(job.id))


if __name__ == "__main__":
    demo_qsub()
    demo_multi_qsub()
