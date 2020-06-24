import os

POSTGRES_URL = os.environ.get('POSTGRES_URL', 'postgresql://postgres:postgres@localhost/test_nplus1loader')
MYSQL_URL = os.environ.get('MYSQL_URL', 'mysql+pymysql://mysql:mysql@localhost/test_nplus1loader')
