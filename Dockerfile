# We can use the same JDK on Intelij IDEA that we used to build JARs
# as the same JDK in our image to run JARs. We don't need anything more
FROM amazoncorretto:11.0.24 AS java
FROM apache/airflow:2.9.3

COPY --from=java /usr/lib/jvm/java-11-amazon-corretto /usr/lib/jvm/java-11-amazon-corretto
ENV JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto/bin
ENV PATH=$PATH:$JAVA_HOME

RUN pip install apache-airflow-providers-apache-spark

EXPOSE 8080
EXPOSE 9443

ENTRYPOINT [ "/bin/bash" ]
# docker build -t my-airflow .
