FROM python:3

MAINTAINER scott@peshak.net

COPY . /app
WORKDIR /app

RUN pip install pipenv
RUN pipenv install --system --deploy

CMD ["python", "export_sensors.py"]
