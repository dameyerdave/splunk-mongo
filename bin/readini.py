import configparser
from pprint import pprint

config = configparser.ConfigParser()
config.read('../default/props.conf')
for key, value in config['adhfile'].items():
    print("%s = %s" % (key, value))
