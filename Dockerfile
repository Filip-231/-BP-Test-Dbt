FROM ubuntu:20.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update
RUN apt-get install -y --no-install-recommends git software-properties-common make build-essential ca-certificates libpq-dev nano
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && apt-get install -y python3.8
RUN apt-get update
RUN apt-get install -y python3-pip

RUN pip3 install --upgrade pip setuptools
COPY requirements.txt ./
RUN pip3 install --requirement ./requirements.txt

COPY . /dbt
WORKDIR /dbt
RUN dbt deps

CMD [ "dbt", "build"]