
DROP TABLE Traders;

CREATE TABLE Traders(

    trader_id INT AUTO_INCREMENT,
    fname VARCHAR(25),
    lname VARCHAR(25),
    date_created DATETIME DEFAULT NOW(),
    password VARCHAR(20),

    PRIMARY KEY(trader_id)
);

ALTER TABLE Traders AUTO_INCREMENT = 1000;
