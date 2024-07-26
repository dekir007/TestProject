import csv
import os
import numpy as np
import psycopg2
import requests
import pandas as pd
from io import StringIO
import logging
from tqdm import tqdm
from mailru_api import MailruCloudFileStreamLinkGenerator
from dotenv import load_dotenv
import pandera as pa
from pandera import Column, DataFrameSchema, Check


def get_filestream_link(public_url: str):
    try:
        logging.info('Started generating filestream_link')
        file_stream_link_generator = MailruCloudFileStreamLinkGenerator(public_url)
        file_stream_link = file_stream_link_generator.file_stream_link

        logging.info('Successful generating filestream_link')
        return file_stream_link
    except Exception as e:
        logging.error(f'Error while generating filestream_link:{e}')
    return None


def download_csv(url: str, chunk_size=1024):
    try:
        logging.info("Started csv download")
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Проверка на ошибки
        total = int(response.headers.get('content-length', 0))

        size = 0
        text: str = ''
        with tqdm(
                total=total,
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
        ) as bar:
            for data in response.iter_content(chunk_size=chunk_size):
                size += len(data)
                text += data.decode('UTF-8')
                bar.update(size)
                # без этого в docker log'ах ничего не видно
                # дополнительно после progress bar спамит кол-во загруженного: "28.0kiB [00:00, 5.21MiB/s]" и тд.
                logging.info(bar)

        logging.info("Successful csv download")
        return text
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading the file: {e}")
        return None


def validate_dataframe(df):
    schema = DataFrameSchema({
        '<TICKER>': Column(str, nullable=False),
        # оказалось, что нужно аж лямбду в лямбде использовать, чтобы работала валидация...
        '<PER>': Column(str, Check(lambda s: s.apply(lambda x: not x.isdigit() and len(x) == 1)), nullable=False),
        # не стал мучать с проверкой с помощью перевода строки в дату. Если там 6 цифр, то должно распарситься
        '<DATE>': Column(str, Check(lambda s: s.apply(lambda x: x.isdigit() and len(x) == 6)), nullable=False),
        '<TIME>': Column(str, nullable=False),
        '<OPEN>': Column(float, Check(lambda x: x >= 0), nullable=False),
        '<HIGH>': Column(float, Check(lambda x: x >= 0), nullable=False),
        '<LOW>': Column(float, Check(lambda x: x >= 0), nullable=False),
        '<CLOSE>': Column(float, Check(lambda x: x >= 0), nullable=False),
        '<VOL>': Column(int, Check(lambda x: x >= 0), nullable=False)
    })
    try:
        validated_df = schema.validate(df)
        logging.info("Data validation successful")
        return validated_df
    except pa.errors.SchemaError as e:
        logging.error(f"Data validation error: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error while data validation: {e}")
        return None


def preprocess_csv(csv_text):
    try:
        logging.info("Started csv preprocessing")
        csv_data = StringIO(csv_text)
        # потому что '000000' превращается в один 0
        df = pd.read_csv(csv_data, sep=';',
                         dtype={'<TIME>': np.str_, '<DATE>': np.str_, '<TICKER>': np.str_, '<PER>': np.str_},
                         encoding='utf8')

        # exception is handled inside the function
        if validate_dataframe(df) is None:
            return

        # решил все-таки через pandas предобрабатывать весь столбец (через apply lambda дольше в ~2 раза)
        df['<DATE>'] = pd.to_datetime(df['<DATE>'], format='%y%m%d')

        logging.info("Successful csv preprocessing")
        return df.values
    except Exception as e:
        logging.error(f"Error processing the CSV file: {e}")
        return None


def get_postgres_connection():
    host = os.getenv('HOST')
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    dbname = os.getenv('DBNAME')
    port = os.getenv('PORT')
    conn = psycopg2.connect(host=host, user=user,
                            password=password,
                            dbname=dbname, port=port)
    return conn


def create_table_imoex_if_not_found(cur):
    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('imoex',))
    if not cur.fetchone()[0]:  # cur.fetchone()[0] returns true if exists
        logging.info(f"Table imoex doesn't exists, creating...")

        try:
            query = os.getenv('CREATE_TABLE')
            cur.execute(query)
        except Exception as e:
            logging.error(f"Error while trying to create table imoex: {e}")
            return False
        logging.info(f"Table imoex was created successfully")
    return True


def load_df_values_to_postgres(cur, df_values):
    sio = StringIO()
    writer = csv.writer(sio)
    writer.writerows(df_values)
    sio.seek(0)

    # мы не хотим сами задавать id
    cur.copy_from(sio, 'imoex', sep=',',
                  columns=('ticker', 'per', 'date', 'time', 'open', 'high', 'low', 'close', 'vol'))


def load_to_postgresql(df_values):
    conn = None
    try:
        logging.info("Started load to Postgresql")
        conn = get_postgres_connection()

        cur = conn.cursor()

        # check if imoex exists and create if doesn't; return False when get error creating it
        if not create_table_imoex_if_not_found(cur):
            logging.error(f"Table wasn't created. Execution stopped.")
            return

        cur.execute("truncate table imoex")

        load_df_values_to_postgres(cur, df_values)

        conn.commit()
        cur.close()

        logging.info("Successful load to Postgresql")
    except Exception as e:
        logging.error(f"Error loading to Postgresql: {e}")
    finally:
        if conn:
            conn.close()


def main():
    load_dotenv()

    logging.basicConfig(level=logging.INFO,
                        # filename="py_log.log",
                        # filemode="w",
                        format="%(asctime)s %(levelname)s %(message)s")

    url = os.getenv('URL')

    if not url:
        logging.error("URL environment variable not found or empty. Execution stopped.")
        return

    filestream_link = get_filestream_link(url)
    if not filestream_link:
        logging.error("Got no filestream_link after get_filestream_link. Execution stopped.")
        return

    csv_text = download_csv(filestream_link)
    if not csv_text:
        logging.error("No data was found when downloaded. Execution stopped.")
        return

    df_values = preprocess_csv(csv_text)
    if df_values is None:
        logging.info("Error when preprocessing. Execution stopped.")
        return

    load_to_postgresql(df_values)


if __name__ == '__main__':
    main()
