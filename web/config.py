

USERNAME = 'wayne'
PASSWORD = 'wayne'
HOST = '192.168.142.140'
PORT = '3306'
DATABASE = 'pipeline'

DATABASE_DEBUG = True



URL = "mysql+pymysql://{}:{}@{}:{}/{}?{}".format(USERNAME, PASSWORD,
            HOST, PORT, DATABASE,
            'charset=utf8')




