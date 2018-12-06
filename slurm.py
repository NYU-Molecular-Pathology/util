#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Convert SLURM squeue output to JSON format
"""
import subprocess as sp
from collections import defaultdict

def parse_SLURM_table(proc_stdout):
    """
    Convert the table formated output of SLURM sinfo, squeue, etc., commands into a list of dicts

    Parameters
    ----------
    proc_stdout: str
        the stdout of a SLURM sinfo or squeue command

    Returns
    -------
    list
        a list of dicts, one for each line in the input table
    """
    lines = proc_stdout.split('\n')
    header_line = lines.pop(0)
    header_cols = header_line.split('|')
    header_cols = [ x.strip() for x in header_cols ]
    entries = []
    error_lines = [] # do something with this later
    for line in lines:
        parts = line.split('|')
        d = {}
        if len(parts) != len(header_cols):
            error_lines.append((len(parts), line, parts))
        else:
            for i, key in enumerate(header_cols):
                d[key] = parts[i]
            entries.append(d)
    return(entries)

class Squeue(object):
    """
    import slurm
    x = slurm.Squeue()
    x.get()
    """
    def __init__(self):
        pass

    def get(self):
        """
        """
        process = sp.Popen(['squeue', '-o', '%all'], stdout = sp.PIPE, stderr = sp.PIPE, shell = False, universal_newlines = True)
        proc_stdout, proc_stderr = process.communicate()
        entries = parse_SLURM_table(proc_stdout)
        return(entries)

class Sinfo(object):
    """
    import slurm
    x = slurm.Sinfo()
    x.get()
    """
    def __init__(self):
        pass

    def get(self):
        """
        """
        process = sp.Popen(['sinfo', '-o', '%all'], stdout = sp.PIPE, stderr = sp.PIPE, shell = False, universal_newlines = True)
        proc_stdout, proc_stderr = process.communicate()
        entries = parse_SLURM_table(proc_stdout)
        return(entries)

class Nodes(object):
    """
    Get the state of the nodes in the cluster

    import slurm; x = slurm.Nodes(); print(x.cpus())
    """
    def __init__(self):
        self.nodes = defaultdict(list)
        for entry in self.get_sinfo():
            self.nodes[entry['HOSTNAMES']].append(entry)

    def cpus(self):
        d = {}
        for node in self.nodes.keys():
            d[node] = self.get_cpu_usage(hostname = node)
        return(d)

    def get_cpu_usage(self, hostname):
        """
        Count of CPUs with this particular configuration by CPU state in the form "available/idle/other/total".
        """
        cpu_usage = list(set([ x["CPUS(A/I/O/T)"].strip() for x in self.nodes[hostname] ]))
        parts = cpu_usage[0].split('/')
        d = {
        'available': parts[0],
        'idle': parts[1],
        'other': parts[2],
        'total': parts[3],
        }
        return(d)

    def get_sinfo(self):
        process = sp.Popen(['sinfo', '-N' , '-o', '%all'], stdout = sp.PIPE, stderr = sp.PIPE, shell = False, universal_newlines = True)
        proc_stdout, proc_stderr = process.communicate()
        entries = parse_SLURM_table(proc_stdout)
        return(entries)

class Partitions(object):
    """
    Get the states of all partitions in the cluster

    import slurm; x = slurm.Partitions(); print(x.most_idle_nodes()); print(x.most_available_nodes())
    """
    def __init__(self):
        self.partitions = {}
        for entry in self.get_sinfo():
            p = entry.pop('PARTITION')
            self.partitions[p] = entry

    def get_sinfo(self):
        process = sp.Popen(['sinfo', '-O', 'nodeaiot,partitionname'], stdout = sp.PIPE, stderr = sp.PIPE, shell = False, universal_newlines = True)
        proc_stdout, proc_stderr = process.communicate()
        lines = proc_stdout.split('\n')
        header = lines.pop(0)
        header_cols = header.split()
        entries = []
        for line in lines:
            parts = line.split()
            d = {}
            # print(parts)
            if len(parts) == len(header_cols):
                for i, header_col in enumerate(header_cols):
                    d[header_col] = parts[i]
                entries.append(d)
            else:
                pass # do something here
        return(entries)

    def get_node_usage(self, partition):
        """
        Count of nodes with this particular configuration by node state in the form "available/idle/other/total".
        """
        node_usage = self.partitions[partition]['NODES(A/I/O/T)']
        parts = node_usage.split('/')
        d = {
        'available': parts[0],
        'idle': parts[1],
        'other': parts[2],
        'total': parts[3],
        }
        return(d)

    def nodes(self):
        """
        Get usage of all nodes in the partitions
        """
        d = {}
        for partition in self.partitions.keys():
            d[partition] = self.get_node_usage(partition = partition)
        return(d)

    def most_idle_nodes(self):
        """
        Check which partition has the most idle nodes
        """
        nodes = self.nodes()
        idle_nodes = {}
        for node in nodes.keys():
            idle_nodes[node] = int(nodes[node]['idle'])
        return(max(idle_nodes, key = idle_nodes.get))

    def most_available_nodes(self):
        """
        Check which partition has the most available nodes
        """
        nodes = self.nodes()
        available_nodes = {}
        for node in nodes.keys():
            available_nodes[node] = int(nodes[node]['available'])
        return(max(available_nodes, key = available_nodes.get))


def all_ints(x):
    return(all([ is_int(y) for y in x ]))

def is_int(y):
    try:
        int(y)
        return(True)
    except ValueError:
        return(False)

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
    n = Nodes()
    print("Partition with the most idle nodes: {0}".format(p.most_idle_nodes()))
    print("Partition with the most available nodes: {0}".format(p.most_available_nodes()))
    print("Node CPU usage:")
    for cpu in n.cpus().keys():
        print("{0}: {1}".format(cpu, n.cpus()[cpu]))
