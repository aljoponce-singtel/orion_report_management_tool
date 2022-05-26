import sys
import importlib
from datetime import datetime

try:
    # Print All arguments
    # sys.argv[0] = This file (manage.py)
    # sys.argv[1] - Report/script folder
    # sys.argv[2] - Main file/module
    # sys.argv[3] - function to call
    print(sys.argv)

    # Add script folder path to python system path
    sys.path.insert(0, './scripts/' + sys.argv[1])
    # Import module
    importModule = importlib.import_module(sys.argv[2])
    # Call function from the imported module
    func = getattr(importModule, sys.argv[3])
    func()

except Exception as error:
    print(error)

    # Output error to file
    timeStamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fileName = '{}.error.{}'.format(__file__, timeStamp)
    errorFile = open(fileName, 'w')
    errorFile.write(str(error))
