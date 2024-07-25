# Тестовое задание по практикуму DWH

## Содержание:
- [Описание](#описание)
- [Демонстрация](#демонстрация)
- [Установка](#установка)
- [Схема БД](#схема-бд)
- [Как это работает](#как-это-работает)

### Описание

Загрузчик данных в БД PostgreSQL на языке Python. 

По публичной ссылке к csv файлу на облаке mail.ru генерирует забирает его, проводит небольшую предобработку и загружает в БД. 

### Демонстрация

Логи контейнера с Python скриптом:

![Логи программы](imgs/Docker_Desktop_xCDwOwHexp.png)

Результат загрузки:

![Загруженные данные](imgs/dbeaver_KcrfbVacF7.png)

### Установка

0. Предполагается, что у вас установлен <code>docker</code>.
1. Скопируйте репозиторий
```
git clone https://github.com/dekir007/TestProject
```

> [!NOTE]
> Если у вас нет инструмента для работы с git, просто скачайте репозиторий по зеленой кнопке Code -> Download ZIP

2. Настройте <code>.env</code> файл на уровне папки <code>TestProject</code> 

Вот этот файл:
```
TestProject/
│
├── src/
│   └── ...
├── initdb/
│   └── ...
├── ...
├── .env
└── ...
```

> [!WARNING]
> Рекомендуется заменить название БД, имя и пароль <code>superuser</code> и <code>testproject</code> пользователя, через которого скрипт будет подключаться к БД, на свои!

Необходимые переменные окружения:
```
DBNAME='postgres'
POSTGRES_USER='postgres'
POSTGRES_PASSWORD='postgres'
PORT='5432'
TESTPROJECT_USER='testproject'
TESTPROJECT_PASSWORD='123123'
```
3. Настройте <code>.env</code> файл на уровне папки <code>src</code>

Вот этот файл:
```
TestProject/
│
├── src/
│   ├── .env
│   └── ...
├── initdb/
│   └── ...
└── ...
```

Здесь <code>POSTGRES_USER</code> и <code>POSTGRES_PASSWORD</code> используются для python скрипта. То есть подставляем сюда значения из <code>TESTPROJECT_USER</code> и <code>TESTPROJECT_PASSWORD</code> из пункта выше.

> [!IMPORTANT]
> Именно здесь задается публичная ссылка на облако!

Необходимые переменные окружения:
```
DBNAME='postgres'
POSTGRES_USER='testproject'
POSTGRES_PASSWORD='123123'
HOST='db'
PORT='5432'
URL='https://cloud.mail.ru/public/L1xB/nvgHGYJz5'
```

> [!NOTE]
> Да, неприятное дублирование, я пробовал сделать один файл. Первый из 2-го пункта использует докер, а этот нужен для скрипта. И проблема в том, что в Dockerfile нельзя прописать COPY .env, он его не копирует! Буду благодарен, если скажите, почему.

4. В консоли на уровне папки <code>TestProject</code> пропишите:

```
docker-compose up
```

После чего поднимутся 2 контейнера: БД и скрипт, который сразу выполнится. Логи выполнения можно увидеть сразу в консоли, потому что мы не прописали флаг <code>-d</code>, запускающий в фоновом режиме. 

> [!TIP]
> Этот проект буквально мой первый опыт работы с докером. Слышал, что ожидание python контейнера поднятия БД контейнера не гарантирует, что БД будет сразу доступна (какое-то время будет инициализация или что-то еще, соединение не будет открыто). Поэтому прописывают bash скрипты для проверки, поднялась ли БД. Взял эту технику [отсюда](https://youtu.be/jCvmvWgKKSw). UPD: оказалось, это правда помогает!

### Схема БД

![Схема БД](imgs/dbeaver_UdpgARoVc1.png)

Скрипт создания таблицы <code>imoex</code>:

```
CREATE TABLE public.imoex (
        id serial4 NOT NULL,
        ticker varchar NOT NULL,
        per bpchar(1) NOT NULL,
        "date" date NOT NULL,
        "time" time NOT NULL,
        "open" money NOT NULL,
        high money NOT NULL,
        low money NOT NULL,
        "close" money NOT NULL,
        vol int8 NOT NULL,
        CONSTRAINT imoex_pk PRIMARY KEY (id)
    );
```

### Как это работает

Создаются 2 докер контейнера: PostgreSQL и Python окружение. 

#### PostgreSQL

Так как по условию задания было необходимо создать пользователя с надлежащими правами доступа, я создал папку initdb, в которой лежит sql скрипт, вызываемый в пункте command в docker-compose.yaml. 

#### Python скрипт


