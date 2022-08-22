import sys
import configparser
from logging.handlers import TimedRotatingFileHandler
import logging.config
import logging

config = configparser.ConfigParser()


def initialize(configPath):
    config.read(configPath)
    logConfig = config['LOG']

    # applies to all modules using this variable
    global logger
    logger = logging.getLogger()
    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleFormat = logging.Formatter(
        fmt="%(asctime)s - %(module)s - %(levelname)s - %(message)s", datefmt="%T")
    consoleHandler.setFormatter(consoleFormat)
    logger.addHandler(consoleHandler)

    if logConfig.getboolean('logToFile') != False:
        fileHandler = TimedRotatingFileHandler(logConfig['logFile'], 'D', 1)
        fileFormat = logging.Formatter(
            fmt="%(asctime)s - %(module)s - %(levelname)s - %(message)s", datefmt="%F %a %T")
        fileHandler.setFormatter(fileFormat)
        logger.addHandler(fileHandler)

    logger.setLevel(getLevelNumValue(logConfig['level']))


def getLevelNumValue(level):

    if level.casefold() == 'info':
        return 20
    elif level.casefold() == 'debug':
        return 10
    elif level.casefold() == 'warning':
        return 10
    elif level.casefold() == 'error':
        return 10
    elif level.casefold() == 'critical':
        return 10
    else:
        return 20
