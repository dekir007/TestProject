FROM python:latest

WORKDIR /src

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONBUFFERED 1

COPY requirements.txt .

#RUN echo "Contents of requirements.txt:" && cat requirements.txt

#RUN pip install python-dotenv

RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY ./src src

#CMD [ "python", "./main.py" ]