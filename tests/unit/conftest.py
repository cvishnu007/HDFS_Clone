"""
conftest.py — ensures namenode/ directory is on sys.path for all unit tests.
This is the pytest-idiomatic way: no per-file sys.path.insert needed.
"""
import sys
import os

# Add namenode/ to path so `from namenode import ...` works
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../namenode'))
