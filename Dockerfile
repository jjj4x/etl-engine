FROM python:3.7.10-slim-buster

WORKDIR /opt/project

COPY requirements.txt requirements.dev.txt ./

RUN \
    pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir -r requirements.dev.txt

ARG DEBIAN_FRONTEND=noninteractive
ARG APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=yes

RUN \
    mkdir -p /usr/share/man/man1 \
    && apt-get update --yes \
    && apt-get install --yes gnupg wget software-properties-common \
    && wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - \
    && add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ \
    && apt-get update --yes \
    && apt-get install --yes adoptopenjdk-8-hotspot \
    && apt-get purge --yes gnupg wget software-properties-common \
    && apt-get autoclean --yes && apt-get autoremove --yes && rm -rf /var/lib/apt/lists/*

ENV \
    JAVA_HOME=/usr/lib/jvm/adoptopenjdk-8-hotspot-amd64 \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HIVE_HOME=/opt/hive \
    HIVE_CONF_DIR=/opt/hive/conf \
    SPARK_CONF_DIR=/opt/spark \
    PATH="${PATH}:/opt/hadoop/bin:/opt/hadoop/sbin:/opt/hive/bin"

COPY README.rst setup.py ./

COPY src/ ./src

RUN pip install --no-deps --editable . && useradd --create-home --system hadoop

USER hadoop
