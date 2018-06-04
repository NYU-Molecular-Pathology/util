#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
unit tests for the samplesheet module
"""
import unittest
import samplesheet

samplesheet1 = "fixtures/SampleSheet.csv"
samplesheet_bad_samples = "fixtures/SampleSheet-Bad1.csv"
samplesheet_bad_lines = "fixtures/SampleSheet-Bad2.csv"
samplesheet_dups = "fixtures/SampleSheet-dup.csv"

class TestSuperFilter(unittest.TestCase):
    def test_true(self):
        self.assertTrue(True, 'Demo assertion')

    def test_fail(self):
        self.assertFalse(False)

    def test_error(self):
        self.assertRaises(ValueError)

    def test_IEMFile_sheet_is_valid1(self):
        sheet_obj = samplesheet.IEMFile(path = samplesheet1)
        self.assertTrue(sheet_obj.isValid(), "Valid samplesheet returns invalid status")

    def test_IEMFile_sheet_invalid_lines1(self):
        sheet_obj = samplesheet.IEMFile(path = samplesheet_bad_lines)
        self.assertRaises(ValueError, sheet_obj.isValid, _raise = True)

    def test_IEMFile_sheet_invalid_samples1(self):
        sheet_obj = samplesheet.IEMFile(path = samplesheet_bad_samples)
        self.assertRaises(ValueError, sheet_obj.isValid, _raise = True)

    def test_IEMFile_duplicate_samples1(self):
        sheet_obj = samplesheet.IEMFile(path = samplesheet_dups)
        self.assertRaises(ValueError, sheet_obj.isValid, _raise = True)

if __name__ == '__main__':
    unittest.main()
