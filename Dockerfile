FROM python:3.12-slim

# Diretório principal do projeto
WORKDIR /app

# Define o diretório do Airflow
ENV AIRFLOW_HOME=/app/airflow

ENV PATH="${PATH}:/root/.local/bin"

# Instala dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    libssl-dev \
    libpq-dev \
    libsasl2-dev \
    libldap2-dev \
    build-essential \
    curl \
    git \
    bash \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Atualiza pip e ferramentas essenciais
RUN pip install --upgrade pip setuptools wheel

# Instala o Airflow com constraints
ARG AIRFLOW_VERSION=2.9.0
ARG PYTHON_VERSION=3.12
RUN pip install apache-airflow==${AIRFLOW_VERSION} \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

# Copia o arquivo requirements.txt para dentro da imagem
COPY requirements.txt /requirements.txt

# Instala as dependências adicionais via requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Expondo a porta padrão do Airflow Webserver e postgres
EXPOSE 8080
EXPOSE 5432

