#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Functions for parsing samplesheets
"""
import re
import csv
from collections import defaultdict
import json
import xml.etree.ElementTree as ET

class IEMFile(object):
    """
    Class for parsing an IEMFileVersion 4 SampleSheet.csv samplesheet used for Illumina NextSeq sequencer into a Python object

    https://www.illumina.com/content/dam/illumina-marketing/documents/products/technotes/sequencing-sheet-format-specifications-technical-note-970-2017-004.pdf

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

        # check if the sheet is valid
        y.isValid(_raise = True)
    """
    def __init__(self, path, debug = False):
        self.path = path
        self.data = self.load_data(path = self.path)
        self.validations = self.get_validations()


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

    def validate_lines(self):
        """
        Checks the samplesheet file and data to make sure it follows standard spec listed here:
        https://www.illumina.com/content/dam/illumina-marketing/documents/products/technotes/sequencing-sheet-format-specifications-technical-note-970-2017-004.pdf

        Notes
        -----
        This function will first check every character in every line in the file

        Returns
        -------
        dict:
            a dictionary of key:value pairs in the format of 'illegal line': ['illegal characters']
        """
        # ~~~~~~~~~~~~~~ VALIDATION CRITERIA ~~~~~~~~~~~~~~~~~~~~ #
        permitted_in_file_chars = (
        '\n', '\r', ' ', "!", '"', "#", "$", "%", "&", "'", "(", ")", "*", "+", ",", "-", ".", "/",
        "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
        ":", ";", "<", "=", ">", "?", "@",
        "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P",
        "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
        "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
        "[", "]", "^", "_", "`", "{", "|", "}", "~",
        "\\" # \
        )
        permitted_in_file_codes = tuple([ ord(i) for i in permitted_in_file_chars ])
        """
        Valid Sample Sheet files are encoded in unicode transformation format, 8 bit (UTF-8) without byte order mark (BOM). A specific list of characters is permitted in the file (Table 1).
        """
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
        # check every line in the file for illegal characters
        illegal_lines = defaultdict(list)
        with open(self.path) as f:
            for line in f:
                for character in line:
                    if ( character not in permitted_in_file_chars ) or ( ord(character) not in permitted_in_file_codes ):
                        illegal_lines[line].append(character)
        return(illegal_lines)

    def validate_sampleIDs(self):
        """
        Checks the samplesheet file and data to make sure it follows standard spec listed here:
        https://www.illumina.com/content/dam/illumina-marketing/documents/products/technotes/sequencing-sheet-format-specifications-technical-note-970-2017-004.pdf

        Notes
        -----
        This function will check the characters and length of each value in the Sample_ID column

        Returns
        -------
        tuple
            a tuple in format of ( ['illegal Sample IDs'], dict( 'Sample ID': ['illegal characters'] ) )
        """
        # ~~~~~~~~~~~~~~ VALIDATION CRITERIA ~~~~~~~~~~~~~~~~~~~~ #
        permitted_in_Sample_ID_codes = (
        48, 49, 50, 51, 52, 53, 54, 55, 56, 57,
        65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90,
        97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122,
        45,
        95
        )
        permitted_in_Sample_ID_chars = tuple([ chr(i) for i in permitted_in_Sample_ID_codes ])
        Sample_ID_char_len_limit = 100
        """
        The field for the Sample_ID column has special character restrictions as only alphanumeric (ASCII codes 48-57, 65- 90, and 97-122), dash (ASCII code 45), and underscore (ASCII code 95) are permitted. The Sample_ID length is limited to 100 characters maximum.
        """
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #

        # check every Sample_ID value in the Data
        illegal_samples_len = []
        illegal_samples_char = defaultdict(list)
        samples = self.data['Data']['Samples']
        for sample in samples:
            sample_ID = sample['Sample_ID']
            ID_length = len(sample_ID)
            if ID_length > Sample_ID_char_len_limit:
                illegal_samples_len.append(sample_ID)
            for character in sample_ID:
                if ( character not in permitted_in_Sample_ID_chars ) or ( ord(character) not in permitted_in_Sample_ID_codes ):
                    illegal_samples_char[sample_ID].append(character)
        return( illegal_samples_len, illegal_samples_char )


    def get_validations(self):
        """
        Gathers the validation information about the object and checks for the presence of errors
        """
        illegal_lines = self.validate_lines()
        illegal_samples_len, illegal_samples_char = self.validate_sampleIDs()

        any_errors = False
        if len(illegal_lines.keys()) > 0:
            any_errors = True
        if len(illegal_samples_len) > 0:
            any_errors = True
        if len(illegal_samples_char.keys()) > 0:
            any_errors = True
        validations = {
        'illegal_lines': illegal_lines,
        'illegal_samples_len': illegal_samples_len,
        'illegal_samples_char': illegal_samples_char,
        'any_errors': any_errors
        }
        return(validations)

    def isValid(self, _raise = False):
        """
        Checks if the samplesheet is valid, based on validation data
        """
        if self.validations['any_errors'] == True:
            if _raise:
                raise ValueError("ERROR: Illegal characters or values present in samplesheet;\n{0}".format(self.validations))
            # samplesheet is not valid if errors are present
            return(False)
        elif self.validations['any_errors'] == False:
            return(True)
        else:
            raise ValueError("ERROR: Unrecognized value for self.validations['any_errors']: {0}".format(self.validations['any_errors']))

    def __repr__(self):
        return(self)
    def __str__(self):
        return(json.dumps(self.data))
    def __len__(self):
        return(len(self.data))

class RunParametersXML(object):
    """
    Class for parsing a RunParameters.xml file generated by BaseSpace from an Illumnia NextSeq

    Examples
    --------
    Example usage::

        x = '/ifs/data/molecpathlab/quicksilver/180131_NB501073_0032_AHT5F3BGX3/RunParameters.xml'
        y = samplesheet.RunParametersXML(path = x)
        y.data
        # {'InstrumentID': 'NB501073', 'ComputerName': 'NEXTSEQ', 'OutputFolder': 'T:\\180131_NB501073_0032_AHT5F3BGX3\\', 'RunStartDate': '180131', 'RunNumber': '32', 'IsPairedEnd': 'true', 'BaseSpaceRunId': '63794731', 'RunID': '180131_NB501073_0032_AHT5F3BGX3', 'RunFolder': 'D:\\Illumina\\NextSeq Control Software Temp\\180131_NB501073_0032_AHT5F3BGX3\\', 'LibraryID': 'NS18-4', 'ExperimentName': 'NS18-4'}
    """
    def __init__(self, path, keys = None):
        self.path = path
        if not keys:
            self.keys = ['RunID', 'ExperimentName', 'LibraryID', 'IsPairedEnd',
                        'InstrumentID', 'RunStartDate', 'ComputerName',
                        'BaseSpaceRunId', 'RunNumber', 'OutputFolder', 'RunFolder']
        self.data = self.load_data(path = self.path)

    def load_data(self, path = None, keys = None):
        """
        Loads the data from the XML file

        Parameters
        ----------
        path: str
            path to the SampleSheet.csv file
        keys: list
            a list of XML keys to find values for

        Returns
        -------
        dict
            a dictionary of the file contents for the selected keys
        """
        if not path:
            path = self.path
        if not keys:
            keys = self.keys
        tree = ET.parse(path)
        root = tree.getroot()
        params_dict = {}
        for key in keys:
            params_dict[key] = root.find(key).text
        return(params_dict)


class SamplesFastqRawCSV(object):
    """
    Class for parsing a samples.fastq-raw.csv file generated by sns pipeline

    Examples
    --------
    Example usage::

        x = '/ifs/data/molecpathlab/NGS580_WES/NS17-02/results_2017-05-23_17-38-30/samples.fastq-raw.csv'
        y = samplesheet.SamplesFastqRawCSV(path = x)
        y.samples
        # ... list of samples ...

    """
    def __init__(self, path):
        self.path = path
        self.samples = self.load_samples(path = self.path)

    def load_samples(self, path = None):
        """
        Loads the samples from the samplesheet

        Parameters
        ----------
        path: str
            path to the samples.fastq-raw.csv file

        Returns
        -------
        list
            a list of the IDs for the samples in the sheet
        """
        if not path:
            path = self.path
        samples = set()
        with open(path) as f:
            for line in f:
                parts = line.strip().split(',')
                samples.add(parts[0])
        return(list(samples))

class SamplesPairsCSV(object):
    """
    Class for parsing a samples.pairs.csv file generated by sns pipeline

    Examples
    --------
    Example usage::

        x = '/ifs/data/molecpathlab/NGS580_WES/NS17-02/results_2017-05-23_17-38-30/samples.pairs.csv'
        y = samplesheet.SamplesPairsCSV(path = x)
        y.pairs
        # ... list of samples pairs dicts ...

    """
    def __init__(self, path):
        self.path = path
        self.pairs = self.load_pairs(path = self.path)

    def load_pairs(self, path = None):
        """
        Loads the sample pairs from the samplesheet

        Parameters
        ----------
        path: str
            path to the samples.pairs.csv file

        Returns
        -------
        list
            a list of dicts containing the Tumor - Normal pairs in the sheet
        """
        if not path:
            path = self.path
        pairs = []
        with open(path) as f:
            reader = csv.DictReader(f, delimiter = ',')
            for row in reader:
                sample = {}
                sample['Normal'] = row['#SAMPLE-N']
                sample['Tumor'] = row['#SAMPLE-T']
                # clean the contents
                for key, value in sample.items():
                    if value == 'NA':
                        sample[key] = None
                pairs.append(sample)
        return(pairs)
