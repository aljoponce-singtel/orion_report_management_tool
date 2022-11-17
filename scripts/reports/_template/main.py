import os
import sys
import logging

# Add the parent directory to sys.path
currentDir = os.path.dirname(os.path.realpath(__file__))
parentDir = os.path.dirname(currentDir)
sys.path.append(parentDir)

# Import modules from their relative path
from _helper import OrionReport

logger = logging.getLogger(__name__)
configFile = os.path.join(os.path.dirname(__file__), 'config.ini')


def main():
    orionReport = OrionReport(configFile)


if __name__ == '__main__':
    main()
