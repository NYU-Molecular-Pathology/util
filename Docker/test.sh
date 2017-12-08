#!/bin/bash
echo "echo Running test from $HOSTNAME" | qsub ; qstat
