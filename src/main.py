import numpy as np
import psycopg2
import requests
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime
import logging


def download_csv(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Проверка на ошибки
        logging.info("Successful csv download")
        return response.text
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading the file: {e}")
        return None


def preprocess_csv(csv_text):
    try:
        csv_data = StringIO(csv_text)
        # потому что '000000' превращается в один 0
        df = pd.read_csv(csv_data, sep=';', dtype={'<TIME>': np.str_})
        # решил все-таки через pandas предобрабатывать весь столбец
        df['<DATE>'] = df['<DATE>'].apply(lambda x: datetime.strptime(str(x), '%y%m%d').date())

        csv_data_processed = StringIO()

        df.to_csv(csv_data_processed, sep=';', index=False, header=False)

        csv_data_processed.seek(0)

        logging.info("Successful csv preprocessing")
        return csv_data_processed
    except Exception as e:
        logging.error(f"Error processing the CSV file: {e}")
        return None


def load_to_postgresql(csv_data_processed):
    try:
        conn = psycopg2.connect(host='db', user='postgres',
                                password='postgres',
                                dbname='postgres', port=5432)

        cur = conn.cursor()
        cur.execute("truncate table imoex")
        cur.copy_from(csv_data_processed, 'imoex', sep=';',
                      columns=('ticker', 'per', 'date', 'time', 'open', 'high', 'low', 'close', 'vol'))

        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.DatabaseError as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

    logging.info("Successful load to Postgresql")


def main():
    logging.basicConfig(level=logging.INFO,
                        #filename="py_log.log",
                        #filemode="w",
                        format="%(asctime)s %(levelname)s %(message)s")

    # нашел через devtools (в network по цепочке событий)
    url = 'https://cloclo62.cloud.mail.ru/public/2CiSLC9HGgbY19MAFts/g/no/L1xB/nvgHGYJz5'  #'https://cloud.mail.ru/public/L1xB/nvgHGYJz5'

    csv_text = download_csv(url)
    if csv_text is None:
        return

    csv_data_processed = preprocess_csv(csv_text)
    if csv_data_processed is None:
        return

    load_to_postgresql(csv_data_processed)

    # f = open("py_log.log")
    # print(f.read())
    # f.close()


if __name__ == '__main__':
    main()
