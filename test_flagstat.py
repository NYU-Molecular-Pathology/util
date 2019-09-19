#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tests for the flagstat module
"""
import os
import flagstat
import unittest

demo_flagstat = os.path.join('fixtures','Sample1.bam.txt')
"""
6922 + 0 in total (QC-passed reads + QC-failed reads)
16 + 0 secondary
0 + 0 supplementary
0 + 0 duplicates
6922 + 0 mapped (100.00% : N/A)
6906 + 0 paired in sequencing
3454 + 0 read1
3452 + 0 read2
6896 + 0 properly paired (99.86% : N/A)
6906 + 0 with itself and mate mapped
0 + 0 singletons (0.00% : N/A)
8 + 0 with mate mapped to a different chr
8 + 0 with mate mapped to a different chr (mapQ>=5)
"""

class TestFlagstat(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        setUp class for the tests; this will only execute once and will be available for
        the tests to access the results
        """
        super(TestFlagstat, cls).setUpClass()
        cls.flagstat = flagstat.Flagstat(txt = demo_flagstat)

    def test_true(self):
        self.assertTrue(True, 'Demo assertion')

    def test_total_mapped_reads1(self):
        self.assertTrue(self.flagstat.vals['TotalMappedReads'] == 6922, 'Total mapped reads do not match expected value')

    def test_SequencedPairedReads1(self):
        self.assertTrue(self.flagstat.vals['SequencedPairedReads'] == 6906, 'Number of Sequenced Paired Reads do not match expected value')

    def test_ProperlyPairedReads1(self):
        self.assertTrue(self.flagstat.vals['ProperlyPairedReads'] == 6896, 'Number of Properly Paired Reads do not match expected value')

    def test_ProperlyPairedPcnt1(self):
        self.assertTrue(self.flagstat.vals['ProperlyPairedPcnt'] == 0.9985519837822183, 'Properly Paired Reads Percent do not match expected value')
