import configparser
import os


class ConfigEngine:

    def __init__(self):
        self._instance = configparser.ConfigParser()
        self._instance.read(os.curdir + r'\\resources\\config.ini')
