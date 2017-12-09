#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module with functions for dealing with .vcf files
"""
import csv
import itertools

# ~~~~~ FUNCTIONS ~~~~~ #
def header_skip_num(vcf_file):
    """
    Counts the number of rows in the header that should be skipped when reading in the file. Header rows in a .vcf start with ``##``

    Parameters
    ----------
    vcf_file: str
        the path to a .vcf file

    Returns
    -------
    int
        the number of rows of header at the beginning of the file
    """
    skip_rows = 0
    with open(vcf_file, 'r') as f:
        for line in f:
            if line.startswith('##'):
                skip_rows += 1
            else:
                break
    return(skip_rows)

def num_entries(vcf_file):
    """
    Counts the number of entries in a .vcf file

    Parameters
    ----------
    vcf_file: str
        the path to a .vcf file

    Returns
    -------
    int
        the number of entries in the file
    """
    num = 0
    skip_rows = header_skip_num(vcf_file = vcf_file)
    with open(vcf_file) as f:
        reader = csv.DictReader(itertools.islice(f, skip_rows, None), delimiter = '\t')
        for row in reader:
            num += 1
    return(num)
