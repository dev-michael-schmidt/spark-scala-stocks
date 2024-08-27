TODO this is ugly.  These MUST happen:

- **Full round trip API -> DB -> Grafana**
- prevent env var duplication for both python and scala
  - split env vars needed for one DAG, but not others
- generate JAR's to a folder, pick up the JAR's here
- Time stamps are hardcoded and de not reflect projects time/date data (i.e use now() or similar)


THEN
  - load more than one symbol in parallel
  - Add table for SMA, EMA