

DROP TABLE tick_data;

CREATE TABLE tick_data(

    ticker VARCHAR(6), 
    date VARCHAR(15),   -- 20xx-xx-xx
    time VARCHAR(15),   -- HH::MM:SS
    type VARCHAR(5),    -- last, bid, ask
    price float,
    size INT,

    FOREIGN KEY(ticker) REFERENCES tickers(ticker_name)

);

CREATE INDEX ix_ticker_date ON tick_data(ticker, date);
CREATE INDEX ix_ticker ON tick_data(ticker);
CREATE INDEX ix_date ON tick_data(date);

