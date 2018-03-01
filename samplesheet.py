#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Functions for parsing samplesheets
"""
import re
import csv
from collections import defaultdict
import json

class IEMFile(object):
    """
    Class for parsing an IEMFileVersion 4 SampleSheet.csv samplesheet used for Illumina NextSeq sequencer into a Python object

    Examples
    --------
    Example usage::

        x = "SampleSheet.csv"
        y = samplesheet.IEMFile(path = x)
        print(y)
        y.path
        # 'SampleSheet.csv'
        y.data['Data']['Samples']
        # ... list of sample dicts

    """
    def __init__(self, path):
        self.path = path
        self.data = self.load_data(path = self.path)

    def load_data(self, path = None):
        """
        Loads the data from the samplesheet into a dictionary

        Parameters
        ----------
        path: str
            path to the SampleSheet.csv file

        Returns
        -------
        dict
            a dictionary of the file contents
        """
        if not path:
            path = self.path
        data = defaultdict(dict)
        with open(path) as f:
            i = 0
            key = None
            for line in f:
                # check for key line
                if line.startswith('['):
                    # parse key name; "[Data],,,,,," -> "Data"
                    key = re.sub(r'^\[(.*)\].*$', r'\1', line.strip())
                    data[key]['line'] = i
                else:
                    # parse the line as an entry for the last active key
                    if key and key != "Data":
                        parts = line.strip().split(',')
                        if len(parts) > 0:
                            # first entry is the name of the values on the line
                            line_key = parts.pop(0)
                            # make sure its not an empty line
                            if line_key != '':
                                # check if there are any entries left that are not empty spreadsheet cells
                                if len([ p for p in parts if p != '' ]) > 0:
                                    data[key][line_key] = parts.pop(0)
                                else:
                                    data[key][line_key] = None
                i += 1

        # get the samples 'Data' from sheet
        data_line = data['Data']['line']
        data['Data']['Samples'] = []
        with open(path) as f:
            for i in range(data_line + 1):
                f.next()
            reader = csv.DictReader(f)
            for row in reader:
                data['Data']['Samples'].append(row)
        return(data)

    def __repr__(self):
        return(self.data)
    def __str__(self):
        return(json.dumps(self.data))
    def __len__(self):
        return(len(self.data))
