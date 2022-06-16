import logging

# Файл, полученный в Google Developer Console
CREDENTIALS_FILE = 'creds.json'
# Сервисы Google
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
# ID Google Sheets документа
SPREADSHEET_ID = '1VtS7f9dT_4wAlpweCD9siEyBvXvQiF9_l5aUlrBZmLg'
# URL ЦБ для получения курса валют
URL = 'http://www.cbr.ru/scripts/XML_daily.asp'
# Параметры подключения к базе данных
HOST = 'localhost'
PORT = '5433'
USER = 'postgres'
PASSWORD = '1111'

# Создание нового логгера и установка уровня логирования 'error' другим логгерам
logging.basicConfig(level='DEBUG')
logger = logging.getLogger()
for log in ('urllib3', 'googleapiclient', 'oauth2client'):
    logging.getLogger(log).setLevel('ERROR')
