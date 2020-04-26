
-- DROP TABLE Trades;
DROP TABLE Algorithm;


CREATE TABLE Algorithm(

    algorithm_id INT AUTO_INCREMENT, -- PRIMARY KEY
    portfolio_id INT, -- FOREIGN KEY
    algorithm_name VARCHAR(50),
    date_created DATETIME DEFAULT NOW(),
    date_stopped DATETIME,
    total_profit float DEFAULT 0,
    updateOn DATETIME DEFAULT NOW(),
    isDeleted INT DEFAULT 0,
   
    PRIMARY KEY(algorithm_id),
    FOREIGN KEY(portfolio_id) REFERENCES Portfolio(portfolio_id)
);

ALTER TABLE Algorithm AUTO_INCREMENT = 1000;
