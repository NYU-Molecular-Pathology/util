#!/usr/bin/env python

'''
This script provides a flexible wrapper for mailing files from a remote server with mutt

USAGE: mutt.py -s "Subject line" -r "address1@gmail.com, address2@gmail.com" -rt "my.address@internets.com" -m "This is my email message" /path/to/attachment1.txt /path/to/attahment2.txt

example mutt command which will be created:
# reply-to field; PUT YOUR EMAIL HERE
export EMAIL="kellys04@nyumc.org"
recipient_list="address1@gmail.com, address2@gmail.com"
mutt -s "$SUBJECT_LINE" -a "$attachment_file" -a "$summary_file" -a "$zipfile" -- "$recipient_list" <<E0F
email message HERE
E0F
'''


# ~~~~ LOAD PACKAGES ~~~~~~ #
import sys
import os
import subprocess as sp
import argparse

# ~~~~ CUSTOM FUNCTIONS ~~~~~~ #
def subprocess_cmd(command):
    '''
    Runs a terminal command with stdout piping enabled
    '''
    import subprocess as sp
    process = sp.Popen(command,stdout=sp.PIPE, shell=True)
    proc_stdout = process.communicate()[0].strip()
    print(proc_stdout)

def make_attachement_string(attachment_files):
    '''
    Return a string to use to in the mutt command to include attachment files
    ex:
    -a "$attachment_file" -a "$summary_file" -a "$zipfile"
    '''
    attachment_strings = []
    if len(attachment_files) > 0:
        for file in attachment_files:
            file_string = '-a "{0}" '.format(file)
            attachment_strings.append(file_string)
    attachment_string = ''.join(attachment_strings)
    return(attachment_string)

def get_file_contents(file):
    '''
    Return a string containing all lines in the file
    '''
    lines_list = []
    with open(file) as f:
        for line in f:
            lines_list.append(line)
    return(''.join(lines_list))

def mutt_mail(recipient_list, reply_to = '', subject_line = '[mutt.py]', message = '~ This message was sent by the mutt.py email script ~', message_file = None, attachment_files = [], return_only_mode = False, quiet = False):
    '''
    Main control function for the program
    Send the message with mutt

    recipient_list = character string; Format is 'address1@gmail.com, address2@gmail.com'
    '''
    if message_file != None:
        message = get_file_contents(message_file)
    attachment_string = make_attachement_string(attachment_files)
    command = '''
export EMAIL="{0}"

mutt -s "{1}" {2} -- "{3}" <<E0F
{4}
E0F'''.format(reply_to, subject_line, attachment_string, recipient_list, message) # message.replace('\n', "$'\n'")
    if quiet == False: print('Email command is:\n{0}\n'.format(command))
    if return_only_mode == False:
        if quiet == False: print('Running command, sending email...')
        subprocess_cmd(command)
    elif return_only_mode == True:
        return(command)

def run():
    '''
    Run the monitoring program
    arg parsing goes here, if program was run as a script
    '''

    # ~~~~ GET SCRIPT ARGS ~~~~~~ #
    parser = argparse.ArgumentParser(description='Mutt email wrapper')

    # required flags
    parser.add_argument("-r", type = str, required=True, dest = 'recipient_list', metavar = 'recipient_list', help="Email(s) to be included in the recipient list. Format is 'address1@gmail.com, address2@gmail.com' ") # nargs='+'

    # optional positional args
    parser.add_argument("attachment_files", type = str,  nargs='*', help="Files to be attached to the email") # nargs='+' # default = [],  action='append', nargs='?',

    # optional flags
    parser.add_argument("-s", default = '[mutt.py]', type = str, dest = 'subject_line', metavar = 'subject_line', help="Subject line for the email")
    parser.add_argument("-m", default = '~ This message was sent by the mutt.py email script ~', type = str, dest = 'message', metavar = 'message', help="Message for the body of the email")
    parser.add_argument("-rt", default = '', type = str, dest = 'reply_to', metavar = 'message', help="'Reply To' email address to display in the 'From' field of the email")
    parser.add_argument("-mf", default = None, type = str, dest = 'message_file', metavar = 'message file', help="File containing text to be included in the message body of the email. Overrides message passed with '-m' argument.")

    parser.add_argument("--norun", default = False, action='store_true', dest = 'return_only_mode',  help="Return the 'mutt' command without running it")
    parser.add_argument("--quiet", default = False, action='store_true', dest = 'quiet',  help="Dont print the command being run")


    args = parser.parse_args()

    recipient_list = args.recipient_list
    attachment_files = args.attachment_files
    subject_line = args.subject_line
    message = args.message
    message_file = args.message_file
    reply_to = args.reply_to
    return_only_mode = args.return_only_mode
    quiet = args.quiet

    mutt_mail(recipient_list = recipient_list, reply_to = reply_to, subject_line = subject_line, message = message, message_file = message_file, attachment_files = attachment_files, return_only_mode = return_only_mode, quiet = quiet)


if __name__ == "__main__":
    run()
