TODO - these are ugly.  These MUST SOON happen:
- **Full round trip API -> DB -> Grafana**
  - ~~generate JAR's to a folder, pick up the JAR's here~~
  - BIG ONE: create dockerfile for spark app
  - BIG ONE: create dockerfile for sss_scheduler! (NEEDS REVIEW)
    - it MUST contain the required python packages
    - it MUST contain the required directory layout
  - BUILD PATTERN so far
    - postgres server dockerfile to create database and table!
      - currently: 
        - `psql -U airflow postgres`
        - `CREATE DATABASE stocks;`
        - `CREATE TABLE` (maybe DG at this point)
        - run sss_scheduler with db init (possibly a RUN in dockerfile)
        - run sss_schedule with scheduler
        - start sss_webserver (if not running)
        - create user

MORE - these push the project forward.  These NEED to happen
  - load more than one symbol in parallel
    - build script (at least partial)
      - for postgres
      - for airflow
    - prevent env var duplication for both python and scala
      - split env vars needed for one DAG, but not others
    - multi-module pom.xml build
      - first: build packages / jars / libs
      - second: table for SMA (for now)
      - third: investigate timestamp as "primary key" (use anti join to see what gets missed)
      - four multi-module, create one module: SMA
      - five test/debug SMA lib + table
    - Time stamps are hardcoded and de not reflect projects time/date data (i.e use now() or similar)
  

POLISH
  - pom.xml plugin variables to "properties," then use variable interpolation 
  - <appendAssemblyId>false</appendAssemblyId> <!-- Update descriptorRef value to be used as the filename
  - Logging