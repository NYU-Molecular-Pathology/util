#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Run all the unit tests
"""

import unittest
import sys

if __name__ == "__main__":
    loader = unittest.TestLoader()
    start_dir = '.'
    suite = loader.discover(start_dir)

    runner = unittest.TextTestRunner()
    ret = not runner.run(suite).wasSuccessful()
    sys.exit(ret)
