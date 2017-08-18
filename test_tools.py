#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
unit tests for the find module
'''
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



if __name__ == '__main__':
    unittest.main()
