import time
import requests
import httplib2
import pandas as pd
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine, exc
from googleapiclient.discovery import build
from sqlalchemy_utils import create_database, database_exists
from oauth2client.service_account import ServiceAccountCredentials

from settings import CREDENTIALS_FILE, SCOPES, SPREADSHEET_ID, URL, HOST, PORT, USER, PASSWORD, logger


def get_usd_exchange_rate() -> float:
    """Получает курс USD с сайта ЦБ"""
    try:
        response = requests.get(url=URL)
        root = ET.fromstring(response.text)
        for valute in root:
            if valute.find('CharCode').text == 'USD':
                rate = float(valute.find('Value').text.replace(',', '.'))
                logger.info('Курс доллара получен!')
                return rate
    except Exception as ex:
        logger.error('Нет соединения с сайтом ЦБ РФ!')
        logger.error(ex)


def get_data_spreadsheet() -> list:
    """Получает данные из электронной таблицы Google"""
    # Авторизация и получение service — экземпляр доступа к API
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPES)
    http_auth = credentials.authorize(httplib2.Http())
    try:
        service = build('sheets', 'v4', http=http_auth)
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
                logger.info('Данные из Google таблицы получены!')
                return values
    except Exception as ex:
        logger.error('Нет соединения с сайтом Google Sheets!')
        logger.error(ex)


def create_dataframe(values: list, rate: float):
    """Создает dataframe из 'values' с добавлением колонки 'стоимость,Р' на основе курса USD (rate)"""
    try:
        df = pd.DataFrame(values[1:], columns=values[0]).set_index('№')
        try:
            df = df.astype({'стоимость,$': 'int'})
            df['стоимость,Р'] = round(df['стоимость,$'] * rate, 2)
            return df
        except (TypeError, ValueError) as ex:
            logger.error('Столбец "стоимость,$" заполнен не корректно!')
            logger.error(ex)
    except ValueError:
        logger.error('Один из столбцов пуст!')


def save_to_db(df):
    """Сохраняет dataframe 'df' в БД на основе СУБД PostgreSQL"""
    try:
        engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/test')
        if not database_exists(engine.url):
            create_database(engine.url)
        df.to_sql('test_table', con=engine, if_exists='replace')
        logger.info('Данные сохранены в БД!')
    except exc.OperationalError as ex:
        logger.error('Не удалось подключится к базе данных!')
        logger.error(ex)


def main():
    old_data = []
    while True:
        new_data = get_data_spreadsheet()
        if new_data and new_data != old_data:
            exchange_rate = get_usd_exchange_rate()
            if exchange_rate:
                data_frame = create_dataframe(new_data, exchange_rate)
                if isinstance(data_frame, pd.core.frame.DataFrame):
                    save_to_db(data_frame)
                    old_data = new_data
        time.sleep(10)


if __name__ == '__main__':
    main()
