import configparser

# Load config from local.ini
config = configparser.ConfigParser()
config.read('local.ini')

# Access database config
db_host = config['database']['host']
db_name = config['database']['name']
db_user = config['database']['user']
db_password = config['database']['password']
db_port = config.getint('database', 'port')  # getint ensures it's an integer

# Access app config
app_secret_key = config['app']['secret_key']
app_debug = config.getboolean('app', 'debug')  # getboolean parses True/False


"""
local.ini content
[database]
host = dbhost.mydomain.com
name = mydbname
user = user
password = password
port = 5432

[app]
secret_key = supersecretkey123
debug = True
"""
