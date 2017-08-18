#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
unit tests for the find module
'''
import unittest
import os
from qsub import Job


class TestJob(unittest.TestCase):
    def setUp(self):
        self.scriptdir = os.path.dirname(os.path.realpath(__file__))
        self.fixture_dir = os.path.join(self.scriptdir, "fixtures")
        self.qstat_stdout_all_Eqw_file = os.path.join(self.fixture_dir, "qstat_stdout_all_Eqw.txt")
        self.qstat_stdout_Eqw_qw_file = os.path.join(self.fixture_dir, "qstat_stdout_Eqw_qw.txt")
        self.qstat_stdout_r_Eqw_file = os.path.join(self.fixture_dir, "qstat_stdout_r_Eqw.txt")
        self.got_job_file = os.path.join(self.fixture_dir, "got_job.txt")

        with open(self.qstat_stdout_all_Eqw_file, "rb") as f:
            self.qstat_stdout_all_Eqw_str = f.read()
        # print(self.qstat_stdout_all_Eqw_str)

        with open(self.qstat_stdout_Eqw_qw_file, "rb") as f:
            self.qstat_stdout_Eqw_qw_str = f.read()
        # print(self.qstat_stdout_Eqw_qw_str)

        with open(self.qstat_stdout_r_Eqw_file, "rb") as f:
            self.qstat_stdout_r_Eqw_str = f.read()
        # print(self.qstat_stdout_r_Eqw_str)

        with open(self.got_job_file, "rb") as f:
            self.got_job_str = f.read()
        self.got_job_out = [self.got_job_str]
        # print(self.got_job_out)

        self.debug_job = Job(id = '', debug = True)

    def tearDown(self):
        del self.scriptdir
        del self.fixture_dir

        del self.qstat_stdout_all_Eqw_file
        del self.qstat_stdout_all_Eqw_str

        del self.qstat_stdout_Eqw_qw_file
        del self.qstat_stdout_Eqw_qw_str

        del self.qstat_stdout_r_Eqw_file
        del self.qstat_stdout_r_Eqw_str


    def test_true(self):
        self.assertTrue(True, 'Demo assertion')

    def test_fail(self):
        self.assertFalse(False)

    def test_error(self):
        self.assertRaises(ValueError)

    def test_debug_init_Job(self):
        '''
        Make sure that the 'debug' init setting prevents attributes from being set
        '''
        attributes = [hasattr(self.debug_job, item) for item in ["entry", "status", "state", "is_running", "is_present"]]
        self.assertFalse(any(attributes))


    def test_running_job1(self):
        '''
        Find running job
        id = '2495634'
        self.qstat_stdout_r_Eqw_file

        qstat_stdout_r_Eqw_file = "fixtures/qstat_stdout_r_Eqw.txt"
        with open(qstat_stdout_r_Eqw_file, "rb") as f: qstat_stdout_r_Eqw_str = f.read()
        from qsub import Job
        x = Job(id = '2495634', debug = True)
        '''
        x = Job(id = '2495634', debug = True)
        x._debug_update(qstat_stdout = self.qstat_stdout_r_Eqw_str)
        self.assertTrue(x.is_running)

    def test_job_Eqw(self):
        '''
        Make sure an Eqw job can be identified
        '''
        x = Job(id = '2493898', debug = True)
        x._debug_update(qstat_stdout = self.qstat_stdout_r_Eqw_str)
        status = x.status
        expected = 'Eqw'
        self.assertTrue(status == expected)

    def test_job_Eqw_not_running(self):
        '''
        Make sure an Eqw job is labeled as not running
        '''
        x = Job(id = '2493898', debug = True)
        x._debug_update(qstat_stdout = self.qstat_stdout_r_Eqw_str)
        status = x.status
        expected = 'Eqw'
        self.assertFalse(x.is_running)

    def test_get_job1(self):
        '''
        Test that a job can be retrieved from qstat_stdout
        '''
        x = Job(id = '2495634', debug = True)
        expected = self.got_job_out
        got_job = x.get_job(id = x.id, qstat_stdout = self.qstat_stdout_r_Eqw_str)
        self.assertTrue(got_job == expected)





if __name__ == '__main__':
    unittest.main()
