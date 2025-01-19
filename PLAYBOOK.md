### Start and Setup Postgres
#### Set permissions
- **Ideally**, permissions are set using the host (non-container) UID/GID are passed to the container.  If these match, then both docker and the local use will have access to each folder.
- Make this public for now:
  - dags (airflow)
  - data (postgres, portainer)
  - libs (jars for airflow)
  - logs (airflow, postgres, portainer)
  - plugins (airflow)

#### Start container sss_postgres
- currently untested/incomplete try using build_postgres.sh
  - TODO: possibly create a (pure) python script that will do this for you.

### Setup Apache Airflow Scheduler:
1) Use the `Dockerfile` to initialize Airflow's database.
    - Build the image `docker build -t my-airflow .` (note the dot) at project root.
    - POSSIBLY: use ["/bin/bash"] to get a shell, use `airflow db migrate`, then `airflow scheduler`
    - Make sure the entrypoint will be `[ "airflow", "db", "init" ]`
    - Make sure all directories are 777 (dags, logs, plugins, and libs)ls -l
    - NOTE: the my-airflow image contains Java.  At all costs try to use the "make and version" in both the container and on the local host.
      - Currently, this is:
        - `FROM amazoncorretto:11.0.24` is the Dockerfile directive.
        - `Scala 2.12.x`
2) Use the `Dockerfile` to set up the scheduler:
   - __NOTE__: Change to ENTRYPOINT ["/bin/bash"] will bring up a bash prompt. _**Within 4 seconds you must select Yes (y)**_ to create the database.
   - You should use DataGrip to connect and test the database.


now just:
echo -e "AIRFLOW_UID=$(id -u)" > .env
rm .env
cd ..

this will wipeout the env file.  just: echo -e "AIRFLOW_UID=$(id -u)" and add that  
echo -e "AIRFLOW_UID=$(id -u)" > .env

docker compose up airflow-init
no such service: airflow-init
docker compose up sss-airflow-init