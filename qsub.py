#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
A collection of functions for submitting jobs to the NYUMC SGE compute cluster with 'qsub' from within Python, and monitoring them until completion

based on https://github.com/stevekm/pyqsub/tree/59c607d72a5b41d4804a969f9d543a89a41e39e6

modified for use with run-monitor system
'''
import logging
logger = logging.getLogger("qsub")
logger.debug("loading qsub module")

from collections import defaultdict
import subprocess as sp
import re
import datetime
from time import sleep
import sys
try:
    from sh import qstat
except:
    logger.error("qstat could not be loaded")
import tools as t

# ~~~~ GLOBALS ~~~~~~ #
# possible qsub job states; default is None
job_state_key = defaultdict(lambda: None)
job_state_key['Eqw'] = 'Error' # job in error status that never started running
job_state_key['r'] = 'Running'
job_state_key['qw'] = 'Waiting' # 'queue wait' job waiting to run
job_state_key['t'] = None # ???
job_state_key['dr'] = None # running jobs submitted for deletion


# ~~~~ CUSTOM CLASSES ~~~~~~ #
class Job(object):
    '''
    A class to track a qsub job that has been submitted

    x = qsub.Job('2379768')
    x.running()
    x.present()
    '''
    def __init__(self, id, name = None, debug = False):
        global job_state_key
        self.job_state_key = job_state_key
        self.id = id
        self.name = name
        # add the rest of the attributes as per the update function
        if not debug:
            self._update()

    def get_job(self, id, qstat_stdout = None):
        '''
        Retrieve the job's qstat entry
        '''
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
        '''
        Get the status of the qsub job
        '''
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
        '''
        Get the interpretation of the job's status
        '''
        # defaultdict returns None if the key is not present
        state = job_state_key[str(status)]
        return(state)

    def get_is_running(self, state, job_state_key):
        '''
        Check if the job is considered to be running
        '''
        is_running = False
        if state in ['Running']:
            is_running = True
        return(is_running)

    def get_is_error(self, state, job_state_key):
        '''
        Check if the job is considered to in an error state
        '''
        is_running = False
        if state in ['Error']:
            is_running = True
        return(is_running)

    def get_is_present(self, id, entry = None, qstat_stdout = None):
        '''
        Find out if a job is present in qsub
        '''
        if not entry:
            entry = self.get_job(id = id, qstat_stdout = qstat_stdout)
        if entry:
            return(True)
        else:
            return(False)

    def _update(self):
        '''
        Update the object's status attributes
        '''
        self.qstat_stdout = qstat()
        self.entry = self.get_job(id = self.id, qstat_stdout = self.qstat_stdout)
        self.status = self.get_status(id = self.id, entry = self.entry, qstat_stdout = self.qstat_stdout)
        self.state = self.get_state(status = self.status, job_state_key = self.job_state_key)
        self.is_running = self.get_is_running(state = self.state, job_state_key = self.job_state_key)
        self.is_error = self.get_is_error(state = self.state, job_state_key = self.job_state_key)
        self.is_present = self.get_is_present(id = self.id, entry = self.entry, qstat_stdout = self.qstat_stdout)

    def _debug_update(self, qstat_stdout):
        '''
        Debug update mode with requires a qstat_stdout to be passed
        '''
        self.qstat_stdout = qstat_stdout
        self.entry = self.get_job(id = self.id, qstat_stdout = self.qstat_stdout)
        self.status = self.get_status(id = self.id, entry = self.entry, qstat_stdout = self.qstat_stdout)
        self.state = self.get_state(status = self.status, job_state_key = self.job_state_key)
        self.is_running = self.get_is_running(state = self.state, job_state_key = self.job_state_key)
        self.is_present = self.get_is_present(id = self.id, entry = self.entry, qstat_stdout = self.qstat_stdout)

    def running(self):
        '''
        Return the most recent running state of the job
        '''
        self._update()
        return(self.is_running)

    def error(self):
        '''
        Return the most recent error state of the job
        '''
        self._update()
        return(self.is_error)

    def present(self):
        '''
        Return the most recent presence or absence of the job
        '''
        self._update()
        return(self.is_present)


def submit(verbose = False, *args, **kwargs):
    '''
    Main function for submitting a qsub job
    passes args to 'submit_job'
    returns a Jobs object for the job

    job = submit(command = '', ...)
    '''
    proc_stdout = submit_job(return_stdout = True, verbose = verbose, *args, **kwargs)
    job_id, job_name = get_job_ID_name(proc_stdout)
    job = Job(id = job_id, name = job_name)
    return(job)


def subprocess_cmd(command, return_stdout = False):
    # run a terminal command with stdout piping enabled
    process = sp.Popen(command,stdout=sp.PIPE, shell=True, universal_newlines=True)
     # universal_newlines=True required for Python 2 3 compatibility with stdout parsing
    proc_stdout = process.communicate()[0].strip()
    if return_stdout == True:
        return(proc_stdout)
    elif return_stdout == False:
        logger.debug(proc_stdout)


def get_job_ID_name(proc_stdout):
    '''
    return a tuple of the form (<id number>, <job name>)
    usage:
    proc_stdout = submit_job(return_stdout = True) # 'Your job 1245023 ("python") has been submitted'
    job_id, job_name = get_job_ID_name(proc_stdout)
    '''
    proc_stdout_list = proc_stdout.split()
    job_id = proc_stdout_list[2]
    job_name = proc_stdout_list[3]
    job_name = re.sub(r'^\("', '', str(job_name))
    job_name = re.sub(r'"\)$', '', str(job_name))
    return((job_id, job_name))


def submit_job(command = 'echo foo', params = '-j y', name = "python", stdout_log_dir = '${PWD}', stderr_log_dir = '${PWD}', return_stdout = False, verbose = False, pre_commands = 'set -x', post_commands = 'set +x', sleeps = None, print_verbose = False):
    '''
    Basic format for job submission to the SGE cluster with qsub
    using a bash heredoc format
    '''
    qsub_command = '''
qsub {0} -N {1} -o :{2}/ -e :{3}/ <<E0F
{4}
{5}
{6}
E0F
'''.format(
params,
name,
stdout_log_dir,
stderr_log_dir,
pre_commands,
command,
post_commands
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



def monitor_jobs(jobs = None, kill_err = True):
    '''
    Monitor a list of qsub Job objects for completion
    make sure that all jobs are present in the qstat output
    if a job is absent or is in error state, remove it from the list of jobs
    wait until the list of jobs reaches 0

    this function does not actually check to make sure the jobs are 'running', only that they are present/absent
    and that they aren't in error state

    if a job is present and not in error state, it is assumed to either be 'qw' waiting to run,
    or 'r' running, in both cases it will eventually finish and leave the queue

    jobs in 'Eqw' error state are stuck and will not leave on their own so must be removed and killed ourselves

    jobs is a list of qsub Job objects
    kill_err = kill Jobs left in error state
    '''
    # make sure jobs were passed
    if not jobs or len(jobs) < 1:
        logger.error('No jobs to monitor')
        return()
    # make sure jobs is a list
    if not isinstance(jobs, list):
        logger.error('"jobs" passed is not a list')
        return()

    # jobs in error state; won't finish
    err_jobs = []
    num_jobs = len(jobs)
    logger.info('Monitoring jobs for completion. Number of jobs in queue: {0}'.format(num_jobs))
    while num_jobs > 0:
        # check number of jobs in the list
        if num_jobs != len(jobs):
            num_jobs = len(jobs)
            logger.debug("Number of jobs in queue: {0}".format(num_jobs))
        # check each job for presence & error state
        for i, job in enumerate(jobs):
            if not job.present():
                jobs.remove(job)
            if job.error():
                err_jobs.append(jobs.pop(i))
        sleep(5)
    logger.info('No jobs remaining in the job queue')

    # check if there were any jobs left in error state
    if err_jobs:
        logger.error('{0} jobs left were left in error state. Jobs: {1}'.format(len(err_jobs), [job.id for job in err_jobs]))
        # kill the error jobs with the 'qdel' command
        if kill_err:
            logger.debug('Killing jobs left in error state')
            qdel_command = 'qdel {0}'.format(' '.join([job.id for job in err_jobs]))
            cmd = t.SubprocessCmd(command = qdel_command).run()
            logger.debug(cmd.proc_stdout)
    return()


def find_all_job_id_names(text):
    '''
    Search a multi-line character string for all qsub job messages
    text is a single text that contains multiple line

    example:
    text = '\n\n process sample SeraCare-1to1-Positive\n\n CMD: qsub -q all.q -cwd -b y -j y -N sns.wes.SeraCare-1to1-Positive -M kellys04@nyumc.org -m a -hard -l mem_free=64G -pe threaded 8-16 bash /ifs/data/molecpathlab/scripts/snsxt/sns_output/test/sns/routes/wes.sh /ifs/data/molecpathlab/scripts/snsxt/sns_output/test SeraCare-1to1-Positive\nYour job 3947957 ("sns.wes.SeraCare-1to1-Positive") has been submitted\n\n'

    [(job_id, job_name) for job_id, job_name in find_all_job_id_names(text)]
    >> [('3947957', 'sns.wes.SeraCare-1to1-Positive')]
    '''
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


def old_job_monitor(jobs):
    '''
    Another method for watching jobs
    I dont like this one as much but it seemed to work and gives info on whether the jobs are running or not
    but too much logic too easy to cause bugs
    '''
    # if there are no jobs, exit the function
    if len(jobs) < 1:
        logger.error('No qsub jobs are present in queue for task')
        return()

    # TODO: debug here
    # wait for start
    logger.debug('Waiting for all jobs to start running...')
    # logger.debug([job.running() for job in jobs])
    # logger.debug(all([job.running() for job in jobs]))
    while not all([job.running() for job in jobs]):
        sleep(1)
        # make sure the jobs did not die
        logger.debug('checking jobs present...')
        # logger.debug([not job.present() for job in jobs])
        # logger.debug(all([not job.present() for job in jobs]))
        if all([not job.present() for job in jobs]):
            logger.warning('All jobs exited while waiting for jobs to start')
            break
    # wait for jobs to finish
    # make sure there's at least 1 job running
    logger.debug('Checking if any jobs are still running...')
    # logger.debug([job.running() for job in jobs])
    # logger.debug(any([job.running() for job in jobs]))
    if any([job.running() for job in jobs]):
        logger.debug('Waiting for all jobs to finish...')
        # logger.debug([job.running() for job in jobs])
        # logger.debug(any([job.running() for job in jobs]))
        while any([job.running() for job in jobs]):
            sleep(5)
    # jobs are all done
    if not any([job.running() for job in jobs]) and not any([job.present() for job in jobs]):
        logger.debug('No jobs remaining in the job queue')
    elif not any([job.running() for job in jobs]) and any([job.present() for job in jobs]):
        logger.warning('Some jobs are remaining in the job queue but are not running')
    return()





def demo_qsub():
    '''
    Demo the qsub code functions
    '''
    print('running single-job demo')

    command = '''
    set -x
    cat /etc/hosts
    sleep 10
    '''

    job = submit(command = command, print_verbose = True)
    print('job id: {0}'.format(job.id))
    print('job name: {0}'.format(job.name))

    print('waiting on job to finish')
    monitor_jobs(jobs = [job])
    print('job has finished\n\n')

    return()

def demo_multi_qsub(job_num = 3):
    '''
    Demo the qsub code functions
    '''
    job_num = int(job_num)

    print('running multi-job demo')

    command = '''
    set -x
    cat /etc/hosts
    sleep 10
    sleep $((1 + RANDOM % 10))
    '''

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


if __name__ == "__main__":
    demo_qsub()
    demo_multi_qsub()
