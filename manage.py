import sys
import importlib
from datetime import datetime

try:
    # All arguments
    print(sys.argv)
    # This file (manage.py)
    print(sys.argv[0])
    # Report/script folder
    print(sys.argv[1])
    # Main file/module
    print(sys.argv[2])
    # function to call
    print(sys.argv[3])

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
