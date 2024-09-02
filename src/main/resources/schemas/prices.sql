CREATE TABLE prices (
    id SERIAL PRIMARY KEY NOT NULL,
    event_ts INT NOT NULL,
    symbol VARCHAR(8) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    adj_close DOUBLE PRECISION,
    volume INT  -- See TODO` on spark DataFrame schema.  Currently DoubleType
);