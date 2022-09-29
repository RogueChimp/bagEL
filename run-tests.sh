#!/usr/bin/bash

#TODO ensure requirements are properly installed
# -m unit_test
pytest --cov -W ignore::DeprecationWarning -W ignore::pytest.PytestUnknownMarkWarning --verbose -s --cov-report term-missing