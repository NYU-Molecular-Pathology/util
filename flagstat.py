#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Access data in a samtools flagstat output txt file
"""
import sys
import re

class Flagstat(object):
    """
    Object to parse samtools flagstat output

    $ cat output/Sample1.bam.txt
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
    def __init__(self, txt):
        self.file = txt
        self.parse()

    def parse(self):
        with open(self.file) as f:
            lines = f.readlines()
        lines = [ l.strip() for l in lines ]

        # total number of alignments
        TotalMappedReads_line = [x for x in lines if re.search('mapped \(', x)][0]
        TotalMappedReads = int(TotalMappedReads_line.split(' ')[0])

        # the read is paired in sequencing, no matter whether it is mapped in a pair
        SequencedPairedReads_line = [x for x in lines if re.search(' paired in sequencing', x)][0]
        SequencedPairedReads = int(SequencedPairedReads_line.split(' ')[0])

        # the read is mapped in a proper pair
        ProperlyPairedReads_line = [x for x in lines if re.search(' properly paired \(', x)][0]
        ProperlyPairedReads = int(ProperlyPairedReads_line.split(' ')[0])

        ProperlyPairedPcnt = float(ProperlyPairedReads) / float(SequencedPairedReads)

        self.vals = {
        'TotalMappedReads': TotalMappedReads,
        'SequencedPairedReads': SequencedPairedReads,
        'ProperlyPairedReads': ProperlyPairedReads,
        'ProperlyPairedPcnt': ProperlyPairedPcnt
        }

def main():
    """
    """
    inputFile = sys.argv[1]

    flagstat = Flagstat(txt = inputFile)
    print(flagstat.vals)

if __name__ == '__main__':
    main()
