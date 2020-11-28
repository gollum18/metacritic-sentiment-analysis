from configparser import ConfigParser
from pymongo import MongoClient

class MongoDB:

    default_config = 'sql.cfg'

    def __init__(self, config=None):
        if not config:
            self.config = ConfigParser()
            self.config.read(MongoDB.default_config)
        self.connected = False

    def open(self):
        if not self.connected:
            self.client = MongoClient(self.connection_string())
            self.connected = True

    def close(self):
        if self.connected:
            self.client.close()
            self.connected = False

    def get_host(self):
        mongocfg = self.config['mongodb']
        if 'Host' not in mongocfg:
            raise KeyError
        return mongocfg['Host']

    def get_port(self):
        mongocfg = self.config['mongodb']
        if 'Port' not in mongocfg:
            raise KeyError
        return mongocfg['Port']

    def set_host(self, host):
        if 'mongodb' not in self.config:
            self.config['mongodb'] = dict()
        self.config['mongodb']['Host'] = host

    def set_port(self, port):
        if 'mongodb' not in self.config:
            self.config['mongodb'] = dict()
        self.config['mongodb']['Port'] = port

    def set_config(self, section, key, value):
        if not isinstance(section, str):
            raise TypeError
        if section not in self.config:
            self.config[section] = dict
        self.config[section][key] = value

    def get_config(self, section, key):
        if not isinstance(section, key) or not isinstance(key, str):
            raise TypeError
        if section not in self.config:
            raise KeyError
        sectioncfg = self.config[section]
        if key not in sectioncfg:
            return None
        return sectioncfg[key]

    def connection_string(self):
        host = self.get_host()
        port = self.get_port()
        if not host or not port:
            raise ValueError
        return f'mongodb://{host}:{port}/'

    def get_database(self, database):
        if not isinstance(database, str):
            raise TypeError
        if self.connected:
            return self.client[database]
        else:
            raise ConnectionError
