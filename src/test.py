import os
import numpy as np
import psycopg2
import requests
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime
import logging
from tqdm import tqdm
from mailru_api import MailruCloudFileStreamLinkGenerator
from dotenv import load_dotenv


def get_filestream_link(public_url: str):
    try:
        file_stream_link_generator = MailruCloudFileStreamLinkGenerator(public_url)
        file_stream_link = file_stream_link_generator.file_stream_link

        logging.info('Successful generating filestream_link')
        return file_stream_link
    except Exception as e:
        logging.error(f'Error while generating filestream_link:{e}')
    return None


def download_csv(url: str, chunk_size=1024):
    try:
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
                # но логи загрузки сильно опаздывают, предположительно, только при малом весе файла
                # дополнительно после progress bar спамит только кол-во загруженного "28.0kiB [00:00, 5.21MiB/s]" и тд.
                print(bar)

        logging.info("Successful csv download")
        return text
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading the file: {e}")
        return None


def preprocess_csv(csv_text):
    try:
        csv_data = StringIO(csv_text)
        # потому что '000000' превращается в один 0
        df = pd.read_csv(csv_data, sep=';', dtype={'<TIME>': np.str_}, encoding='utf8')
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
        host = os.getenv('HOST')
        user = os.getenv('POSTGRES_USER')
        password = os.getenv('POSTGRES_PASSWORD')
        dbname = os.getenv('DBNAME')
        port = os.getenv('PORT')
        conn = psycopg2.connect(host='localhost', user=user,
                                password=password,
                                dbname=dbname, port=5433)

        cur = conn.cursor()

        # check if imoex exists
        cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('imoex',))
        if not cur.fetchone()[0]: # cur.fetchone()[0] returns true if exists
            logging.error(f"Table imoex doesn't exists")

            try:
                cur.execute("""CREATE TABLE public.imoex (
                                id serial NOT NULL,
                                ticker varchar NULL,
                                per char(1) NULL,
                                "date" date NULL,
                                "time" time NULL,
                                "open" money NULL,
                                high money NULL,
                                low money NULL,
                                "close" money NULL,
                                vol bigint NULL,
                                CONSTRAINT imoex_pk PRIMARY KEY (id)
                            );
                    """)
            except Exception as e:
                logging.error("Error while trying to create table imoex")

        cur.execute("truncate table imoex")
        cur.copy_from(csv_data_processed, 'imoex', sep=';',
                      columns=('ticker', 'per', 'date', 'time', 'open', 'high', 'low', 'close', 'vol'))

        conn.commit()

        cur.execute("select * from imoex")
        for row in cur.fetchall():
            print(row)

        cur.close()
        conn.close()
        logging.info("Successful load to Postgresql")
    except psycopg2.DatabaseError as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")


def main():
    load_dotenv()

    logging.basicConfig(level=logging.INFO,
                        #filename="py_log.log",
                        #filemode="w",
                        format="%(asctime)s %(levelname)s %(message)s")
    # current_dir = os.path.dirname(os.path.abspath(__file__))
    # parent_dir = os.path.abspath(os.path.join(current_dir, '..'))
    # print(os.listdir(current_dir))
    # print(os.listdir(parent_dir))
    #
    # f = open(os.path.abspath(os.path.join(current_dir, '..', '.dockerenv')))
    # print(f.read())
    # f.close()

    url = os.getenv('URL')
    # print('url ', url)

    filestream_link = get_filestream_link(url)

    csv_text = download_csv(filestream_link)
    if csv_text is None:
        return

    csv_data_processed = preprocess_csv(csv_text)
    if csv_data_processed is None:
        return
    print(csv_data_processed.getvalue())
    load_to_postgresql(csv_data_processed)


if __name__ == '__main__':
    main()
