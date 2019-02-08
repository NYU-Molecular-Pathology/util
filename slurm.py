#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module for working with SLURM on HPC cluster

Tested with SLURM version 17.11.7
"""
import subprocess as sp
from collections import defaultdict

def parse_SLURM_table(stdout):
    """
    Convert the table formated output of SLURM 'sinfo -o '%all', 'squeue -o '%all', etc., commands into a list of dicts

    Parameters
    ----------
    stdout: str
        the stdout of a SLURM sinfo or squeue command

    Returns
    -------
    dict
        yields a dict of entries from each valid line in the stdout
    """
    # split all the stdout lines
    lines = stdout.split('\n')
    # get the headers from the first line
    header_line = lines.pop(0)
    # split the headers apart
    header_cols = header_line.split('|')
    header_cols = [ x.strip() for x in header_cols ]
    # iterate over remaining lines
    for line in lines:
        # split each line
        parts = line.split('|')
        parts = [ x.strip() for x in parts ]
        # start building dict for the values
        d = {}
        # make sure that the stdout line has the same number of fields as the headers
        if len(parts) == len(header_cols):
            # fill in the dict values and yield the results
            for i, header_col in enumerate(header_cols):
                d[header_col] = parts[i]
            yield(d)
        else:
            pass # do something here


class Squeue(object):
    """
    View information about jobs located in the Slurm scheduling queue.

    https://slurm.schedmd.com/squeue.html

    Examples
    ---------

    from util import slurm
    sq = slurm.Squeue()
    sq.get()
    """
    def __init__(self, debug = False):
        if not debug:
            self.update()

    def update(self):
        """
        """
        returncode, entries = self.get_squeue()
        self.returncode = returncode
        self.entries = entries

    def get_squeue(self):
        """
        Get the 'squeue' HPC cluster usage information

        Returns
        -------
        int, list
            integer error code from the 'squeue' command
            a list of dicts representing the 'squeue' values; the case of an error, returns an empty list
        """
        # system command to run; "Print all fields available for this data type with a vertical bar separating each field."
        process = sp.Popen(['squeue', '-o', '%all'], stdout = sp.PIPE, stderr = sp.PIPE, shell = False, universal_newlines = True)
        # run the command, capture stdout and stderr
        proc_stdout, proc_stderr = process.communicate()
        # check the exit status
        if process.returncode == 0:
            entries = [ entry for entry in parse_SLURM_table(stdout = proc_stdout) ]
        else:
            entries = []
        return(process.returncode, entries)

class Sinfo(object):
    """
    Information about Slurm nodes and partitions.

    https://slurm.schedmd.com/sinfo.html

    Examples
    ---------
    import slurm
    x = slurm.Sinfo()
    x.entries
    """
    def __init__(self, debug = False):
        if not debug:
            self.update()

    def update(self):
        """
        """
        returncode, entries = self.get_sinfo()
        self.returncode = returncode
        self.entries = entries

    def get_sinfo(self):
        """
        Get the 'sinfo' HPC cluster usage information

        Returns
        -------
        int, list
            integer error code from the 'sinfo' command
            a list of dicts representing the 'sinfo' values; the case of an error, returns an empty list

        """
        # system command to run; "Print all fields available for this data type with a vertical bar separating each field."
        process = sp.Popen(['sinfo', '-o', '%all'], stdout = sp.PIPE, stderr = sp.PIPE, shell = False, universal_newlines = True)
        # run the command, capture stdout and stderr
        proc_stdout, proc_stderr = process.communicate()
        # check the exit status
        if process.returncode == 0:
            entries = [ entry for entry in parse_SLURM_table(stdout = proc_stdout) ]
        else:
            entries = []
        return(process.returncode, entries)


class Nodes(object):
    """
    Get the state of the nodes in the cluster

    Examples
    --------
    # import util.slurm as slurm
    from util import slurm
    n = slurm.Nodes()

    [ i for i in n.sinfo.entries if i['HOSTNAMES']=='gpu-0006' ]
    n.nodes['gpu-0006']['entries']
    n.avail
    """
    def __init__(self, debug = False):
        self.nodes = defaultdict(dict)
        if not debug:
            self.update()

    def update(self):
        self.sinfo = Sinfo()
        self._get_nodes()
        self.avail = self._get_avail()

    def _get_nodes(self):
        for entry in self.sinfo.entries:
            if 'entries' not in self.nodes[entry['HOSTNAMES']]:
                self.nodes[entry['HOSTNAMES']]['entries'] = []
            self.nodes[entry['HOSTNAMES']]['entries'].append(entry)

            # set values based on each sinfo entry;
            # sinfo may have multiple entries per node but these values should be the same for all of them
            # total resources a node contains
            if 'resources' not in self.nodes[entry['HOSTNAMES']]:
                self.nodes[entry['HOSTNAMES']]['resources'] = {}
            self.nodes[entry['HOSTNAMES']]['resources']['CPUS'] = entry['CPUS']
            self.nodes[entry['HOSTNAMES']]['resources']['SOCKETS'] = entry['SOCKETS']
            self.nodes[entry['HOSTNAMES']]['resources']['MEMORY'] = entry['MEMORY']
            self.nodes[entry['HOSTNAMES']]['resources']['GRES'] = entry['GRES']

            # resources available to the node right now
            if 'avail' not in self.nodes[entry['HOSTNAMES']]:
                self.nodes[entry['HOSTNAMES']]['avail'] = {}
            self.nodes[entry['HOSTNAMES']]['avail']['mem'] = entry['FREE_MEM']
            self.nodes[entry['HOSTNAMES']]['avail']['cpus'] = self.get_cpu_aiot(aiot_str = entry['CPUS(A/I/O/T)'])
            self.nodes[entry['HOSTNAMES']]['avail']['up'] = self.is_up(reason_str = entry['REASON'])
            self.nodes[entry['HOSTNAMES']]['avail']['state'] = entry['STATE']
            if 'partitions' not in self.nodes[entry['HOSTNAMES']]['avail']:
                self.nodes[entry['HOSTNAMES']]['avail']['partitions'] = []
            self.nodes[entry['HOSTNAMES']]['avail']['partitions'].append(entry['PARTITION'])

    def _get_avail(self):
        """
        Get the availability summary for each node
        """
        data = []
        for name, values in self.nodes.items():
            if name == '':
                continue
            if values['avail']['up'] != True:
                continue
            d = {}
            d['node'] = name
            d['cpu'] = values['avail']['cpus']['idle']
            d['state'] = values['avail']['state']
            d['mem'] = values['avail']['mem']
            d['partitions'] = ','.join(values['avail']['partitions'])
            data.append(d)
        data = sorted(data, key=lambda k: k['node'])
        return(data)

    def get_cpu_aiot(self, aiot_str):
        """
        Parse the 'CPUS(A/I/O/T)' field in SLURM sinfo output (allocated, idle, other, total)

        '0/40/0/40'
        """
        parts = aiot_str.split('/')
        d = {
        'allocated': int(parts[0]),
        'idle': int(parts[1]),
        'other': int(parts[2]),
        'total': int(parts[3]),
        }
        return(d)

    def is_up(self, reason_str):
        """
        Check the 'REASON' field to determine if the node is up or down
        """
        if reason_str == 'none':
            return(True)
        else:
            return(False)

            # partition = entry['PARTITION']
            # num_nodes = entry['NODES']
            # num_cpus = entry['CPUS']
#
            # initialize new dict if not already there
#             if partition not in self.partitions:
#                 self.partitions[partition] = {}
#             if state not in self.partitions[partition]:
#                 self.partitions[partition][state] = {}
#             if "num_nodes" not in self.partitions[partition][state]:
#                 self.partitions[partition][state]["num_nodes"] = 0
#
#             # update the number of nodes for each state
#             self.partitions[partition][state]["num_nodes"] += int(num_nodes)

#         self.nodes = defaultdict(list)
#         for entry in self.get_sinfo():
#             self.nodes[entry['HOSTNAMES']].append(entry)
#
#     def cpus(self):
#         d = {}
#         for node in self.nodes.keys():
#             d[node] = self.get_cpu_usage(hostname = node)
#         return(d)
#
#     def get_cpu_usage(self, hostname):
#         """
#         Count of CPUs with this particular configuration by CPU state in the form "available/idle/other/total".
#         """
#         cpu_usage = list(set([ x["CPUS(A/I/O/T)"].strip() for x in self.nodes[hostname] ]))
#         parts = cpu_usage[0].split('/')
        # d = {
        # 'available': parts[0],
        # 'idle': parts[1],
        # 'other': parts[2],
        # 'total': parts[3],
        # }
        # return(d)
#
#     def get_sinfo(self):
#         process = sp.Popen(['sinfo', '-N' , '-o', '%all'], stdout = sp.PIPE, stderr = sp.PIPE, shell = False, universal_newlines = True)
#         proc_stdout, proc_stderr = process.communicate()
#         entries = parse_SLURM_table(proc_stdout)
#         return(entries)

class Partitions(object):
    """
    Get the states of all partitions in the cluster


    Examples
    --------
    import util.slurm as slurm

    p = slurm.Partitions()

    p.partitions
    >>> {'gpu4_short': {'mixed': {'num_nodes': 12}, 'allocated': {'num_nodes': 2}, 'idle': {'num_nodes': 3}}, 'fn_long': {'mixed': {'num_nodes': 1}, 'idle': {'num_nodes': 1}}, 'cpu_long': {'mixed': {'num_nodes': 7}, 'allocated': {'num_nodes': 5}, 'idle': {'num_nodes': 1}, 'drained': {'num_nodes': 1}}, 'gpu4_long': {'mixed': {'num_nodes': 7}, 'allocated': {'num_nodes': 3}, 'idle': {'num_nodes': 1}}, 'fn_medium': {'mixed': {'num_nodes': 1}, 'idle': {'num_nodes': 1}}, 'gpu8_dev': {'mixed': {'num_nodes': 1}, 'idle': {'num_nodes': 1}, 'drained': {'num_nodes': 1}}, 'data_mover': {'idle': {'num_nodes': 3}, 'drained': {'num_nodes': 1}}, 'intellispace': {'mixed': {'num_nodes': 1}, 'idle': {'num_nodes': 1}}, 'dev': {'n/a': {'num_nodes': 0}}, 'gpu8_short': {'mixed': {'num_nodes': 1}, 'idle': {'num_nodes': 2}, 'drained': {'num_nodes': 1}}, 'gpu8_long': {'mixed': {'num_nodes': 1}, 'allocated': {'num_nodes': 1}, 'drained': {'num_nodes': 1}}, 'gpu8_medium': {'mixed': {'num_nodes': 2}, 'drained': {'num_nodes': 1}}, 'cpu_short': {'mixed': {'num_nodes': 12}, 'allocated': {'num_nodes': 2}, 'idle': {'num_nodes': 18}}, 'fn_short': {'mixed': {'num_nodes': 1}, 'idle': {'num_nodes': 1}, 'drained': {'num_nodes': 1}}, 'cpu_medium': {'mixed': {'num_nodes': 15}, 'allocated': {'num_nodes': 3}, 'down*': {'num_nodes': 1}, 'idle': {'num_nodes': 1}}, 'prod': {'n/a': {'num_nodes': 0}}, 'cpu_dev': {'mixed': {'num_nodes': 6}, 'allocated': {'num_nodes': 1}, 'idle': {'num_nodes': 2}, 'drained': {'num_nodes': 1}}, 'gpu4_dev': {'mixed': {'num_nodes': 6}, 'allocated': {'num_nodes': 2}, 'idle': {'num_nodes': 1}, 'drained': {'num_nodes': 1}}, 'gpu4_medium': {'mixed': {'num_nodes': 8}, 'allocated': {'num_nodes': 2}, 'idle': {'num_nodes': 3}}}

    """
    def __init__(self, debug = False):
        self.partitions = {}
        if not debug:
            self.update()

    def update(self):
        self.sinfo = Sinfo()
        self._get_partitions()

    def _get_partitions(self):
        for entry in self.sinfo.entries:
            partition = entry['PARTITION']
            state = entry['STATE']
            num_nodes = entry['NODES']

            # initialize new dict if not already there
            if partition not in self.partitions:
                self.partitions[partition] = {}
            if state not in self.partitions[partition]:
                self.partitions[partition][state] = {}
            if "num_nodes" not in self.partitions[partition][state]:
                self.partitions[partition][state]["num_nodes"] = 0

            # update the number of nodes for each state
            self.partitions[partition][state]["num_nodes"] += int(num_nodes)

    def by_state(self, state, key, **kwargs):
        """
        Get a list of values for partitions in a given state

        Examples
        --------
        >>> p.by_state(state = "idle", key = "num_nodes")
        {'intellispace': 1, 'cpu_dev': 2, 'cpu_short': 18, 'cpu_medium': 1, 'cpu_long': 1, 'fn_short': 1, 'fn_medium': 1, 'fn_long': 1, 'gpu4_dev': 1, 'gpu4_short': 3, 'gpu4_medium': 3, 'gpu4_long': 1, 'gpu8_dev': 1, 'gpu8_short': 2, 'data_mover': 3}
        """
        blacklist = kwargs.pop("blacklist", [])
        d = { part: vals[state][key] for part, vals in self.partitions.items() if state in vals if key in vals[state] }
        for partition in blacklist:
            d.pop(partition, None)
        return(d)

    def most_idle_nodes(self, **kwargs):
        idle_nodes = self.by_state(state = "idle", key = "num_nodes", **kwargs)
        most_idle = None
        if len(idle_nodes) > 0:
            most_idle = max(idle_nodes, key = idle_nodes.get)
        return(most_idle)

    def most_mixed_nodes(self, **kwargs):
        mixed_nodes = self.by_state(state = "mixed", key = "num_nodes", **kwargs)
        most_mixed = None
        if len(mixed_nodes) > 0:
            most_mixed = max(mixed_nodes, key = mixed_nodes.get)
        return(most_mixed)




            # d = {}
            # print(entry['PARTITION'])

    # def get_sinfo(self):
    #     """
    #     Get the 'sinfo' HPC cluster usage information
    #
    #     https://slurm.schedmd.com/sinfo.html
    #
    #     sinfo -o '%P %T %F %C'
    #
    #     Returns
    #     -------
    #     int, list
    #         integer error code from the 'sinfo' command
    #         a list of dicts representing the 'sinfo' values; the case of an error, returns an empty list
    #
    #     """
    #     # system command to run
    #     process = sp.Popen(['sinfo', '-O', 'nodeaiot,partitionname'], stdout = sp.PIPE, stderr = sp.PIPE, shell = False, universal_newlines = True)
    #     # run the command, capture stdout and stderr
    #     proc_stdout, proc_stderr = process.communicate()
    #     # check the exit status
    #     if process.returncode == 0:
    #         entries = [ entry for entry in self.parse_sinfo(stdout = proc_stdout) ]
    #     else:
    #         entries = []
    #     return(process.returncode, entries)
    #
    # def parse_sinfo(self, stdout):
    #     """
    #     Generator that yields dictionaries of '{ sinfo_field: sinfo_value}' pairs
    #
    #     Parameters
    #     ----------
    #     stdout: str
    #         character string representing the stdout of the 'sinfo' command
    #
    #     Returns
    #     -------
    #     dict
    #         yields a dict of entries from each valid line in the stdout
    #
    #     Examples
    #     --------
    #
    #     stdout = 'NODES(A/I/O/T)      PARTITION           \n0/0/0/0             prod                \n0/0/0/0             dev                 \n1/1/0/2             intellispace        \n8/1/1/10            cpu_dev             \n17/15/0/32          cpu_short           \n18/1/1/20           cpu_medium          \n12/1/1/14           cpu_long            \n1/1/1/3             fn_short            \n1/1/0/2             fn_medium           \n1/1/0/2             fn_long             \n9/0/1/10            gpu4_dev            \n14/3/0/17           gpu4_short          \n13/0/0/13           gpu4_medium         \n11/0/0/11           gpu4_long           \n2/0/1/3             gpu8_dev            \n3/0/1/4             gpu8_short          \n2/0/1/3             gpu8_medium         \n2/0/1/3             gpu8_long           \n0/3/1/4             data_mover          \n'
    #
    #     [ i for i in parse_sinfo(stdout) ]
    #     """
    #     # split all the stdout lines
    #     lines = stdout.split('\n')
    #     # get the headers from the first line
    #     header = lines.pop(0)
    #     # split the headers apart
    #     header_cols = header.split()
    #     # iterate over remaining lines
    #     for line in lines:
    #         # split each line
    #         parts = line.split()
    #         # start building dict for the values
    #         d = {}
    #         # make sure that the stdout line has the same number of fields as the headers
    #         if len(parts) == len(header_cols):
    #             # fill in the dict values and yield the results
    #             for i, header_col in enumerate(header_cols):
    #                 d[header_col] = parts[i]
    #             yield(d)
    #         else:
    #             pass # do something here
    #
    # def get_node_usage(self, partition, usage_field = 'NODES(A/I/O/T)'):
    #     """
    #     Count of nodes with this particular configuration by node state in the form "available/idle/other/total".
    #     """
    #     node_usage = self.partitions[partition][usage_field]
    #     parts = node_usage.split('/')
    #     d = {
    #     'available': parts[0],
    #     'idle': parts[1],
    #     'other': parts[2],
    #     'total': parts[3],
    #     }
    #     return(d)
    #
    # def nodes(self):
    #     """
    #     Get usage of all nodes in the partitions
    #     """
    #     d = {}
    #     for partition in self.partitions.keys():
    #         d[partition] = self.get_node_usage(partition = partition)
    #     return(d)
    #
    # def most_idle_nodes(self):
    #     """
    #     Check which partition has the most idle nodes
    #     """
    #     nodes = self.nodes()
    #     idle_nodes = {}
    #     for node in nodes.keys():
    #         idle_nodes[node] = int(nodes[node]['idle'])
    #     return(max(idle_nodes, key = idle_nodes.get))
    #
    # def most_available_nodes(self):
    #     """
    #     Check which partition has the most available nodes
    #     """
    #     nodes = self.nodes()
    #     available_nodes = {}
    #     for node in nodes.keys():
    #         available_nodes[node] = int(nodes[node]['available'])
    #     return(max(available_nodes, key = available_nodes.get))
    #


def is_int(y):
    try:
        int(y)
        return(True)
    except ValueError:
        return(False)

def all_ints(x):
    return(all([ is_int(y) for y in x ]))


def parse_nodelist(nodes_str):
    """
    Convert the NODELIST string from 'sinfo' output to a list of node names

    Examples
    ------
    Example usage::

        parse_nodelist(nodes_str = 'cn-[0006,0011-0014,0016-0024,0026,0045-0054]')
        >>> ['cn-0006', 'cn-0011', 'cn-0012', 'cn-0013', 'cn-0016', 'cn-0017', 'cn-0018', 'cn-0019', 'cn-0020', 'cn-0021', 'cn-0022', 'cn-0023', 'cn-0026', 'cn-0045', 'cn-0046', 'cn-0047', 'cn-0048', 'cn-0049', 'cn-0050', 'cn-0051', 'cn-0052', 'cn-0053']
    """
    # nodes_str = 'cn-[0006,0011-0014,0016-0024,0026,0045-0054]'
    nodes_str_parts = nodes_str.split('[') # ['cn-', '0006,0011-0014,0016-0024,0026,0045-0054]']
    node_prefix = nodes_str_parts.pop(0) # cn-
    nodes_list_str = nodes_str_parts.pop(0) # 0006,0011-0014,0016-0024,0026,0045-0054]
    nodes_list_str = nodes_list_str.strip(']') # 0006,0011-0014,0016-0024,0026,0045-0054
    nodes_list_str_parts = nodes_list_str.split(',') # ['0006', '0011-0014', '0016-0024', '0026', '0045-0054']
    all_node_suffixes = []
    # check if each value is just a string of ints, or needs to be parsed further
    for node_suffix in nodes_list_str_parts:
        if all_ints(node_suffix):
            all_node_suffixes.append(node_suffix)
        else:
            #  try to split on '-'
            node_suffix_parts = node_suffix.split('-')
            # make sure there's at least 2 parts after splitting
            if len(node_suffix_parts) > 1:
                node_start = node_suffix_parts[0]
                node_stop = node_suffix_parts[1]
                # make sure both parts are all ints
                if all_ints(node_start) and all_ints(node_stop):
                    node_vals = [ "{0:04}".format(i) for i in range( int(node_start), int(node_stop) ) ]
                    for node_val in node_vals:
                        all_node_suffixes.append(node_val)
                else:
                    pass # do something here
            else:
                pass # do something here
    node_labels = [ "{0}{1}".format(node_prefix, x) for x in all_node_suffixes ]
    return(node_labels)


if __name__ == '__main__':
    p = Partitions()
    print(p.partitions)
    partition_blacklist = [
    "data_mover",
    "cpu_dev",
    "gpu4_dev",
    "fn_short",
    "fn_medium",
    "fn_long",
    "cpu_long"
    ]
    print("Partition with the most idle nodes: {0}".format(p.most_idle_nodes(blacklist = partition_blacklist)))
    print("Partition with the most mixed nodes: {0}".format(p.most_mixed_nodes(blacklist = partition_blacklist)))
    intellispace_cpus = 0
    sq = Squeue()
    for entry in sq.entries:
        if entry['PARTITION'] == 'intellispace':
            intellispace_cpus += int(entry['CPUS'])
    print("Intellispace CPUs used/requested: {0}".format(intellispace_cpus))
    # n = Nodes()
    # print("Node CPU usage:")
    # for cpu in n.cpus().keys():
    #     print("{0}: {1}".format(cpu, n.cpus()[cpu]))
