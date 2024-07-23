import psycopg2
import requests
import pandas as pd
from io import StringIO
from datetime import datetime


def download_csv(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Проверка на ошибки
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error downloading the file: {e}")
        return None


def process_csv(csv_text):
    try:
        csv_data = StringIO(csv_text)

        df = pd.read_csv(csv_data, sep=';')
        df['DATE'] = df['DATE'].apply(lambda x: datetime.strptime(str(x), '%y%m%d').date())

        csv_data_processed = StringIO()

        df.to_csv(csv_data_processed, sep=';', index=False, header=False)

        csv_data_processed.seek(0)  # Перемещение указателя в начало

        return csv_data_processed
    except Exception as e:
        print(f"Error processing the CSV file: {e}")
        return None


def load_to_postgresql(csv_data_processed, conn_params):
    try:
        conn = psycopg2.connect(**conn_params)
        print(conn)
        cur = conn.cursor()

        cur.copy_from(csv_data_processed, 'stock_data', sep=';',
                      columns=('ticker', 'per', 'date', 'time', 'open', 'high', 'low', 'close', 'vol'))

        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


def main():
    url = 'https://cloclo63.datacloudmail.ru/public/get/QQ6rhqGnP34LDeTxciLjFq5L9eguKH8hQvSiUMN81ZigBh8u2sCNvHXPpt7cj3fywdakJbHhgR7ND84AfPMPqBjNcwB7KWVz3QmepLV3GoaRqXC3ngSXXAJPuuSMdwMwjjycQkQ7HchdKgiXoFSZhU2zJvkBEtYguLr3hEsCPbsUBn7aGfPj7BGERgHAexBcCuaDGcocZZ47LidN6DywvANY7jHLdZznkYbk7eiz47v7Am7cLAT2RDBGC6SRVekDitim6xRjysJ9NX6pt6A3frhfofgRgDRxsRr764yukUEPTw5ZeKnNVUhxi8b142YrqpC476SFVaLUTwsw53ug5wGkFG6EcmLgTiSzpSyuyi6sdLjCo2snUmwpamR5Xca5kHzYYbbTpcqnyBeg/kireev.dmitrii1408@mail.ru/IMOEX_230101_240601.csv'
    conn_params = {
        'host': 'db',
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'postgres',
        'port': '5432'
    }

    csv_text = download_csv(url)
    print(csv_text)
    if csv_text is None:
        return

    csv_data_processed = process_csv(csv_text)
    if csv_data_processed is None:
        return

    load_to_postgresql(csv_data_processed, conn_params)


if __name__ == '__main__':
    main()
