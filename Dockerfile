# Base image to create user with specific UID and GID and java
FROM amazoncorretto:11.0.24-alpine AS sss-base-java
# Create a user with specific UID and GID
ARG UID=1000
RUN addgroup -S appgroup && adduser -S appuser -u $UID -G appgroup

FROM apache/airflow:latest AS sss-airflow-java
COPY --from=sss-base-java /usr/lib/jvm/java-11-amazon-corretto /usr/lib/jvm/java-11-amazon-corretto

# Ensure JAVA_HOME is correctly set
ENV JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto
ENV PATH=$PATH:$JAVA_HOME

# Install the Airflow Spark provider.  This is needed for SparkSubmitOperator
RUN pip install apache-airflow-providers-apache-spark

USER root
RUN apt update && \
    apt upgrade -y && \
    rm -rf /var/lib/apt/lists/*

# docker build --tag sss/airflow-2.10-java11 .

## The ENTRYPOINT directive is implicitly:
## ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
## because this is the ENTRYPOINT in apache/airflow:2.10.0



# to run Airflow tasks as this user
# USER myuser:mygroup

## The ENTRYPOINT directive is implicitly:
## ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
########## END OF STAGE AIRFLOW ##########
#
#
########### STAGE POSTGRES ##########
#FROM postgres:13-alpine as sss-postgres
## While duplicated, we need the t
#ARG UID=1000
#ARG GID=1000
#RUN addgroup -g $GID mygroup && \
#    adduser -u $UID -G mygroup -D myuser
########### ENH OF STAGE POSTGRE ##########
#
#FROM portainer/portainer-ce:2.21.0 AS sss-portainer
#ARG UID=1000
#ARG GID=1000
#RUN addgroup -g $GID mygroup && \
#    adduser -u $UID -G mygroup -D myuser
#
#
#
#
### sss-airflow-java
##FROM apache/airflow:2.10.0 AS sss-airflow-java
##COPY --from=sss-base-java /usr/lib/jvm/java-11-amazon-corretto /usr/lib/jvm/java-11-amazon-corretto
##ENV JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto/bin
##ENV PATH=$PATH:$JAVA_HOME
##RUN pip install apache-airflow-providers-apache-spark
##
### The ENTRYPOINT directive is implicitly:
### ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
### because this is the ENTRYPOINT in apache/airflow:2.10.0
