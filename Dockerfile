FROM ubuntu:latest
COPY ./requirements.txt /app/requirements.txt

WORKDIR /app
RUN yes | apt-get update -y
RUN yes | apt-get install python3-pip -y
RUN pip install --no-cache-dir --upgrade pip
RUN pip install -r requirements.txt

COPY . /app

CMD ["gunicorn", "--bind", "0.0.0.0:5004", "application:app"]
