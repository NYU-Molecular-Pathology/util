#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
unit tests for the find module
"""
import unittest
import os
import tools as t
from tools import DirHop
from tools import SubprocessCmd

scriptfile = os.path.realpath(__file__)
scriptdir = os.path.dirname(os.path.realpath(__file__))
fixture_dir = os.path.join(scriptdir, "fixtures")


class TestDirHop(unittest.TestCase):
    def test_true(self):
        self.assertTrue(True, 'Demo true assertion')

    def test_cwd_change(self):
        with DirHop(fixture_dir) as d:
            new_cwd = os.getcwd()
        self.assertTrue(new_cwd == fixture_dir, "New cwd does not match expected cwd")

    def test_cwd_change_fail(self):
        old_cwd = os.getcwd()
        with DirHop(fixture_dir) as d:
            new_cwd = os.getcwd()
        self.assertFalse(new_cwd == old_cwd, "New cwd matches expected old cwd when it shouldn't")


class TestItemExists(unittest.TestCase):
    def test_item_should_exist_any(self):
        exists = t.item_exists(item = scriptfile, item_type = 'any')
        self.assertTrue(exists)

    def test_item_should_exist_file(self):
        exists = t.item_exists(item = scriptfile, item_type = 'file')
        self.assertTrue(exists)

    def test_item_should_exist_dir(self):
        exists = t.item_exists(item = scriptdir, item_type = 'dir')
        self.assertTrue(exists)

    def test_item_should_not_exist_file(self):
        item = "foobarbaznotarealfilenamefakefilegoeshere"
        exists = t.item_exists(item = item, item_type = 'file')
        self.assertFalse(exists)

    def test_item_wrong_type(self):
        exists = t.item_exists(item = scriptfile, item_type = 'dir')
        self.assertFalse(exists)

class TestSubprocessCmd(unittest.TestCase):
    # def setUp(self):
    #     self.cmd_foo = SubprocessCmd(command = "echo foo")
    #
    # def tearDown(self):
    #     del self.cmd_foo

    def test_cmd_echo_success(self):
        command = "echo foo"
        x = SubprocessCmd(command = command)
        x.run()
        returncode = x.process.returncode
        self.assertTrue(returncode < 1)

    def test_cmd_echo_stdout(self):
        command = "echo foo"
        x = SubprocessCmd(command = command)
        x.run()
        proc_stdout = x.proc_stdout
        self.assertTrue(proc_stdout == 'foo')

    def test_cmd_fail(self):
        command = "foobarbaznotarealcommandzzzzzzzz_thiscommandshouldfail"
        x = SubprocessCmd(command = command)
        x.run()
        returncode = x.process.returncode
        self.assertFalse(returncode < 1)

class TestNumLines(unittest.TestCase):
    def test_num_lines1(self):
        input_file = os.path.join(fixture_dir, 'variants_ref.tsv')
        self.assertTrue(t.num_lines(input_file = input_file, skip = 0) == 367)
    def test_skip(self):
        input_file = os.path.join(fixture_dir, 'variants_ref.tsv')
        self.assertTrue(t.num_lines(input_file = input_file, skip = 1) == 366)

class TestWriteTabularOverlap(unittest.TestCase):
    """
    write_tabular_overlap
    """
    def test_full_overlap(self):
        file1 = os.path.join(fixture_dir, 'variants_head200.tsv')
        ref_file = os.path.join(fixture_dir, 'variants_ref.tsv')
        output_file = os.path.join(fixture_dir, 'foo_{0}.tsv'.format(t.timestamp()))
        t.write_tabular_overlap(file1 = file1, ref_file = ref_file, output_file = output_file)
        num_lines = t.num_lines(input_file = output_file, skip = 0)
        self.assertTrue(num_lines == 201, 'Number of lines output in full overlap files does not match')
    def test_partial_overlap(self):
        file1 = os.path.join(fixture_dir, 'variants_head200.tsv')
        ref_file = os.path.join(fixture_dir, 'variants_tail200.tsv')
        output_file = os.path.join(fixture_dir, 'foo_{0}.tsv'.format(t.timestamp()))
        t.write_tabular_overlap(file1 = file1, ref_file = ref_file, output_file = output_file)
        num_lines = t.num_lines(input_file = output_file, skip = 0)
        self.assertTrue(num_lines == 38, 'Number of lines output in partial overlap files does not match')
    def test_true(self):
        self.assertTrue(True, 'Demo true assertion')

class TestUpdateJSON(unittest.TestCase):
    def test_update_json1(self):
        data1 = {'a': 1, 'b': 2}
        data2 = {'c': 3, 'd': 4}
        data3 = {'a': 1, 'c': 3, 'b': 2, 'd': 4}
        output_file = os.path.join(fixture_dir, 'foo_{0}.json'.format(t.timestamp()))
        t.write_json(object = data1, output_file = output_file)
        t.update_json(data = data2, input_file = output_file)
        data4 = t.load_json(input_file = output_file)
        self.assertTrue(data3 == data4, 'Data read from JSON file does not match expected output')
    def test_update_missingfile(self):
        data1 = {'a': 1, 'b': 2}
        output_file = os.path.join(fixture_dir, 'foo2_{0}.json'.format(t.timestamp()))
        exists_before = t.item_exists(output_file)
        t.update_json(data = data1, input_file = output_file)
        exists_after = t.item_exists(output_file)
        self.assertTrue(exists_after and not exists_before, 'File was not created correctly by the update JSON function')



if __name__ == '__main__':
    unittest.main()
