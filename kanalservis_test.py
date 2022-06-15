import time
import requests
import httplib2
import pandas as pd
import apiclient.discovery
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists
from oauth2client.service_account import ServiceAccountCredentials

from settings import CREDENTIALS_FILE, SCOPES, SPREADSHEET_ID, URL


def get_exchange_rate() -> float:
    """Получает курс USD с сайта ЦБ"""
    try:
        response = requests.get(url=URL)
        root = ET.fromstring(response.text)
        for valute in root:
            if valute.find('CharCode').text in 'USD':
                rate = float(valute.find('Value').text.replace(',', '.'))
                print('Курс доллара получен!')
                return rate
    except Exception as ex:
        print('Нет соединения с сервером!', ex, sep='\n')


def get_data_spreadsheet() -> list:
    """Получает данные из электронной таблицы Google"""
    # Авторизация и получение service — экземпляр доступа к API
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPES)
    http_auth = credentials.authorize(httplib2.Http())
    try:
        service = apiclient.discovery.build('sheets', 'v4', http=http_auth)
        # Чтение файла по 10 строк
        values, i = [], 1
        while True:
            value = service.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f'A{i}:D{i+9}',
                majorDimension='ROWS'
            ).execute()
            if value.get('values'):
                values += value['values']
                i += 10
            else:
                print('Данные Google таблицы получены!')
                return values
    except Exception as ex:
        print('Нет соединения с сервером!', ex, sep='\n')


def save_database(values: list, rate: float):
    """Сохраняет данные (values) в БД с добавлением колонки 'стоимость,Р' на основе курса USD (rate)"""
    df = pd.DataFrame(values[1:], columns=values[0]).set_index('№')
    try:
        df = df.astype({'стоимость,$': 'int'})
        df['стоимость,Р'] = round(df['стоимость,$'] * rate, 2)
        # Сохранение данных в БД на основе СУБД PostgreSQL
        engine = create_engine('postgresql+psycopg2://postgres:1111@localhost:5433/test')
        if not database_exists(engine.url):
            create_database(engine.url)
        df.to_sql('test_table', con=engine, if_exists='replace')
        print('Данные сохранены в БД!')
    except (TypeError, ValueError) as ex:
        print('Столбец "стоимость,$" заполнен не корректно!', ex, sep='\n')


def main():
    old_data = []
    while True:
        new_data = get_data_spreadsheet()
        if new_data and new_data != old_data:
            exchange_rate = get_exchange_rate()
            if exchange_rate:
                save_database(new_data, exchange_rate)
            old_data = new_data
        time.sleep(10)


if __name__ == '__main__':
    main()
