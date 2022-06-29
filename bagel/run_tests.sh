#!/usr/bin/bash

#TODO ensure requirements are properly installe
pytest --cov=src tests/ -W ignore::DeprecationWarning -W ignore::pytest.PytestUnknownMarkWarning --verbose -s -m unit_test  