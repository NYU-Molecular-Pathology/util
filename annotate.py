#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module for annotation of files containing genomic data in .vcf, .bed, formats

reference:
https://github.com/stevekm/reportIT/blob/608242fc3ed426f8a665d8293c377262cbea5aba/code/old/IonTorrent_summary_table.py#L53
https://github.com/stevekm/annotate-peaks/blob/master/ANNOVAR/annotate.R
https://github.com/stevekm/junk-drawer/blob/master/custom_functions_samples.py
"""
# ~~~~~ LOGGING ~~~~~ #
import log
import logging
logger = log.logger_from_configs(name = __name__,
                                primary_config_yaml = 'logging.yml',
                                backup_config_yaml = 'logging.yml',
                                logger_name = "annotate")


# ~~~~~ IMPORT PACKAGES ~~~~~ #
import os
import sys
import find
import tools
import vcf

# ~~~~~ GLOBALS ~~~~~ #
genome = "hg19"

# directory with ANNOVAR installation
ANNOVAR_bin_dir = os.path.join(os.path.expanduser("~"), "annovar")
ANNOVAR_db_dir = os.path.join(ANNOVAR_bin_dir, "db")
ANNOVAR_protocol = "cytoBand,refGene"
ANNOVAR_operation = "r,g"
ANNOVAR_buildver = genome

configs = {
'ANNOVAR_bin_dir': ANNOVAR_bin_dir,
'ANNOVAR_db_dir': ANNOVAR_db_dir,
'ANNOVAR_protocol': ANNOVAR_protocol,
'ANNOVAR_operation': ANNOVAR_operation,
'ANNOVAR_buildver': ANNOVAR_buildver
}




# ~~~~~ FUNCTIONS ~~~~~ #
def vcf2annovar(vcf_file, **kwargs):
    """
    Converts a .vcf file to ANNOVAR .avinput format, using ANNOVAR ``convert2annovar.pl``

    Parameters
    ----------
    vcf_file: str
        the path to a .vcf file

    Keyword Arguments
    -----------------
    output_file: str
        the path to the ``.avinput`` file to be created, or ``None``
    bin_dir: str
        path to the ANNOVAR installation directory, or ``None`` to use the internally set default location

    Notes
    -----
    Generates and executes a shell command in the format::

        annovar/convert2annovar.pl -format vcf4old /data/output/169.duplications.vcf -includeinfo > /data/output/169.duplications.avinput

    Returns
    -------
    str
        the path to the output ``.avinput`` file

    """
    bin_dir = kwargs.pop('bin_dir', configs['ANNOVAR_bin_dir'])
    output_file = kwargs.pop('output_file', os.path.splitext(vcf_file)[0] + '.avinput')

    # make sure input file exists
    tools.missing_item_kill(item = vcf_file, logger = logger)

    # path to binary to use
    convert_bin = os.path.join(bin_dir, 'convert2annovar.pl')
    tools.missing_item_kill(item = convert_bin, logger = logger)

    # shell command to run
    convert_command = '''
"{0}" -format vcf4old "{1}" -includeinfo > "{2}"
    '''.format(
    convert_bin, # 0
    vcf_file, # 1
    output_file # 2
    )

    # run
    logger.debug(convert_command)
    run_cmd = tools.SubprocessCmd(command = convert_command).run()
    logger.debug(run_cmd.proc_stdout)
    logger.debug(run_cmd.proc_stderr)

    # make sure output file exists
    tools.missing_item_kill(item = output_file, logger = logger)

    return(output_file)


def table_annovar(avinput_file, **kwargs):
    """
    Runs ANNOVAR ``table_annovar.pl``

    Parameters
    ----------
    avinput_file: str
        path to ANNOVAR format ``.avinput`` file

    Keyword Arguments
    -----------------
    output_file_base: str
        file path base for the annotated output file; `` `` will be automatically appended by ANNOVAR
    bin_dir: str
        path to the ANNOVAR installation directory
    db_dir: str
        path to the ANNOVAR database directory
    buildver: str
        the build version to use, e.g. "hg19"

    Notes
    -----
    Generates and executes a shell command in the format::

        perl "/annovar/table_annovar.pl" "example-data/Sample1.avinput" "/annovar/db" --outfile "example-data/Sample1" --buildver "hg19" --protocol "cytoBand,refGene" --operation "r,g" --nastring "." --remove

    Returns
    -------
    """
    # get keyword arguments
    bin_dir = kwargs.pop('bin_dir', configs['ANNOVAR_bin_dir'])
    db_dir = kwargs.pop('db_dir', configs['ANNOVAR_db_dir'])
    buildver = kwargs.pop('db_dir', configs['ANNOVAR_buildver'])
    protocol = kwargs.pop('protocol', configs['ANNOVAR_protocol'])
    operation = kwargs.pop('operation', configs['ANNOVAR_operation'])
    output_file_base = kwargs.pop('operation', os.path.splitext(avinput_file)[0])

    # make sure input file exists
    tools.missing_item_kill(item = avinput_file, logger = logger)

    # expected output file
    output_suffix = '.{0}_multianno.txt'.format(buildver)
    multianno_output = output_file_base + output_suffix

    table_annovar_bin = os.path.join(bin_dir, 'table_annovar.pl')

    table_annovar_command = '''
"{0}" "{1}" "{2}" --outfile "{3}" --buildver "{4}" --protocol "{5}" --operation "{6}" --nastring "." --remove
    '''.format(
    table_annovar_bin, # 0
    avinput_file, # 1
    db_dir, # 2
    output_file_base, # 3
    buildver, # 4
    protocol, # 5
    operation # 6
    )

    logger.debug(table_annovar_command)
    run_cmd = tools.SubprocessCmd(command = table_annovar_command).run()
    logger.debug(run_cmd.proc_stdout)
    logger.debug(run_cmd.proc_stderr)

    # make sure output file exists
    tools.missing_item_kill(item = multianno_output, logger = logger)

    return(multianno_output)

def filetype_validation(input_file):
    """
    Runs specific validation steps for certain file types

    Parameters
    ----------
    input_file: str
        the path to a file to be validated.

    Returns
    -------
    bool
        either ``True`` or ``False`` if the file passed validation
    """
    # get the file type from the file's extension
    filetype = os.path.splitext(input_file)[1]
    if filetype == '.vcf':
        # make sure the .vcf has at least 1 entry
        num_entries = vcf.num_entries(vcf_file = input_file)
        if not num_entries > 0:
            logger.warning('VCF file has {0} lines and will not be annotated: {1}'.format(num_entries, input_file))
            return(False)
        else:
            # otherwise it appears to be valid
            return(True)
    else:
        # otherwise it appears to be valid
        return(True)



def validate(input_file):
    """
    Validates a file for annotation. Makes sure that the file meets valdation criteria

    Parameters
    ----------
    input_file: str
        the path to a file to be validated.

    Returns
    -------
    bool
        either ``True`` or ``False`` if the file passed validation

    Notes
    -----
    Criteria:

    - file must exist

    - file must have >0 lines
    """
    # check file existence
    if not tools.item_exists(item = input_file, item_type = 'file'):
        logger.warning('File does not exist and will not be annotated: {0}'.format(input_file))
        return(False)

    # check number if lines
    num_lines = tools.num_lines(input_file)
    if not num_lines > 0:
        logger.warning('File has {0} lines and will not be annotated: {1}'.format(num_lines, input_file))
        return(False)

    # return the boolean value from the filetype specific validations
    return(filetype_validation(input_file))

def validate_ANNOVAR(**kwargs):
    """
    Makes sure that all expected ANNOVAR binary file paths exist

    Keyword Arguments
    -----------------
    bin_dir: str
        path to the ANNOVAR installation directory, or ``None`` to use the internally set default location
    db_dir: str
        path to the ANNOVAR database directory, or ``None`` to use the internally set default location
    """
    # bin_dir = None, db_dir = None, buildver = None
    # get keyword arguments
    bin_dir = kwargs.pop('bin_dir', configs['ANNOVAR_bin_dir'])
    db_dir = kwargs.pop('db_dir', configs['ANNOVAR_db_dir'])

    # list of file paths that must exist for ANNOVAR to be installed & configured properly
    required_paths = []

    # the ANNOVAR bin's used in this module
    annovar_bins = [
    'table_annovar.pl',
    'convert2annovar.pl'
    ]

    # get the full paths to the bins
    annovar_bin_paths = [os.path.join(bin_dir, b) for b in annovar_bins]

    # add to required paths
    required_paths.append(bin_dir)
    required_paths.append(db_dir)
    for annovar_bin_path in annovar_bin_paths:
        required_paths.append(annovar_bin_path)

    # make sure they all exist
    for required_path in required_paths:
        tools.missing_item_kill(item = required_path, logger = logger)
    logger.debug(required_paths)


def main(input_dir):
    """
    Runs annotation on a directory if the module was called as a script

    Todo
    ----
    Need to add checking for more invalid vcf inputs !!!
    
    """
    # make sure ANNOVAR is installed properly
    validate_ANNOVAR()

    # find all vcf files in the supplied input_dir
    inclusion_pattern = '*.vcf'
    logger.debug('Annotating with configs:\n{0}'.format(configs))

    # find the .vcf files
    files = find.find(search_dir = input_dir, inclusion_patterns = (inclusion_pattern,), search_type = 'file')
    logger.debug('found {0} files of type {1}'.format(len(files), inclusion_pattern))

    # expand real full paths
    files = [tools.fullpath(f) for f in files]

    # validate files
    validated_files = [{'file': f, 'is_valid': validate(f)} for f in files]

    for validated_file in validated_files:
        logger.debug(validated_file)
        if validated_file['is_valid']:

            # convert file to .avinput format
            logger.debug('Converting file to ANNOVAR format: {0}'.format(validated_file['file']))
            validated_file['avinput'] = vcf2annovar(vcf_file = validated_file['file'])

            # annotate the file
            logger.debug('Annotating file with ANNOVAR: {0}'.format(validated_file['avinput']))
            validated_file['multianno_output'] = table_annovar(avinput_file = validated_file['avinput'], output_file_base = None)

        else:
            logger.debug('file is not valid and will not be annotated: {0}'.format(validated_file['file']))


def parse():
    """
    Parses CLI args if the module was called as a script

    Examples
    --------
    Example usage::

        ./annotate.py ../output/

    """
    # udpate configs
    molecpathlab_annovar_dir = '/ifs/data/molecpathlab/bin/annovar'
    molecpathlab_annovar_db_dir = os.path.join(molecpathlab_annovar_dir, 'db')
    configs['ANNOVAR_bin_dir'] = molecpathlab_annovar_dir
    configs['ANNOVAR_db_dir'] = molecpathlab_annovar_db_dir

    input_dir = sys.argv[1]
    main(input_dir = input_dir)

# ~~~~~ RUN ~~~~~ #
if __name__ == "__main__":
    parse()
