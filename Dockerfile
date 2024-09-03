##
# This custom Apache Airflow image adds Amazon's corretto-11.0.24. Amazon
# Corretto 11 is a free, multi-platform distribution of OpenJDK 11 that
# is certified to be compatible with the Java SE standard and is a
# Long-Term Supported (LTS) distribution.
#
# This JDK matches in both the dev environment, and the container
# Environment and should ganurtee 100% Java compatibility
FROM apache/airflow:latest

# Ideally, the host and container UID match allowing host access to container resources
ARG UID=1000

# Required:
ENV HADOOP_CONF_DIR=not-used
ENV YARN_CONF_DIR=not-used

# Install the Airflow Spark provider.  This is needed for SparkSubmitOperator
RUN pip install apache-airflow-providers-apache-spark

# Install Java
USER root
RUN apt update && \
    apt install -y wget && \
    wget -O - https://apt.corretto.aws/corretto.key | gpg --dearmor -o /usr/share/keyrings/corretto-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/corretto-keyring.gpg] https://apt.corretto.aws stable main" | tee /etc/apt/sources.list.d/corretto.list && \
    apt update && \
    apt upgrade -y && \
    apt install -y java-11-amazon-corretto-jdk && \
    rm -rf /var/lib/apt/lists/*

## The ENTRYPOINT directive is implicitly (requires 'command:' in compose.yaml
## ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
# docker build --tag sss/airflow-2.10-java11 .



