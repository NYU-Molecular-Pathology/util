#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
unit tests for the find module
'''
import unittest
from find import multi_filter
from find import super_filter

class TestSuperFilter(unittest.TestCase):
    def test_true(self):
        self.assertTrue(True, 'Demo assertion')

    def test_fail(self):
        self.assertFalse(False)

    def test_error(self):
        self.assertRaises(ValueError)

    def test_super_filter_all_Eqw(self):
        filenames = ['fixtures/qstat_stdout_all_Eqw.txt', 'fixtures/qstat_stdout_r_Eqw.txt', 'fixtures/qstat_stdout_Eqw_qw.txt']
        match_result = [x for x in super_filter(names = filenames, inclusion_patterns = "*Eqw*")]
        self.assertTrue(filenames == match_result)

    def test_super_filter_all_Eqw_fail(self):
        filenames = ['fixtures/qstat_stdout_all_Eqw.txt', 'fixtures/qstat_stdout_r_Eqw.txt', 'fixtures/qstat_stdout_Eqw_qw.txt']
        match_result = [x for x in super_filter(names = filenames, inclusion_patterns = "*E1qw*")]
        self.assertFalse(filenames == match_result)



if __name__ == '__main__':
    unittest.main()
