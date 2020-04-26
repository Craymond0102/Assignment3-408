
DROP TABLE Trades;

CREATE TABLE Trades(

    trade_id INT AUTO_INCREMENT, -- PRIMARY KEY
    trader_id INT,               -- FOREIGN KEY
    algorithm_id INT,            -- FOREIGN KEY

    ticker VARCHAR(6),
    order_type VARCHAR(5),      -- MKT/LMT
    price float,
    size INT,
   
    date VARCHAR(25),
    time VARCHAR(25),
 
    PRIMARY KEY(trade_id),
    FOREIGN KEY(trader_id) REFERENCES Traders(trader_id),
    FOREIGN KEY(algorithm_id) REFERENCES Algorithm(algorithm_id)
);

CREATE INDEX ix_ticker_date ON Trades(ticker, date);
CREATE INDEX ix_ticker ON Trades(ticker);
CREATE INDEX ix_date ON Trades(date);

