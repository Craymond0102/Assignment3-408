

DROP TABLE market_tick;

CREATE TABLE market_tick(

    ticker VARCHAR(6),
    date VARCHAR(25),
    time VARCHAR(25),
    price float,
    ask float,
    bid float,
    volume INT,
    ask_size INT,
    bid_size INT,    

    PRIMARY KEY(ticker, date, time),
    FOREIGN KEY(ticker) REFERENCES tickers(ticker_name)
);

CREATE INDEX ix_ticker_date ON market_tick(ticker, date);
CREATE INDEX ix_ticker ON market_tick(ticker);
CREATE INDEX ix_date ON market_tick(date);

