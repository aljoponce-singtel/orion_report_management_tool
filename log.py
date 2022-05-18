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
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%T")
    consoleHandler.setFormatter(consoleFormat)
    fileHandler = TimedRotatingFileHandler(logConfig['logFile'], 'D', 1)
    fileFormat = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%F %a %T")
    fileHandler.setFormatter(fileFormat)
    logger.addHandler(consoleHandler)
    logger.addHandler(fileHandler)
    logger.setLevel(logging.DEBUG)
