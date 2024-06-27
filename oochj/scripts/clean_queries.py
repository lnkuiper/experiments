import os
import sys
import shutil


SCRIPTS_DIR = os.path.dirname(__file__)
sys.path.append(f'{SCRIPTS_DIR}/..')
from benchmark.util.util import *


def main():
    if os.path.exists(QUERIES_DIR):
        shutil.rmtree(QUERIES_DIR)


if __name__ == '__main__':
    main()
