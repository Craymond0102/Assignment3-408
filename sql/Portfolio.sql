

CREATE TABLE Portfolio(

    portfolio_id INT AUTO_INCREMENT,
    pname VARCHAR(25),
    starting_value float,
    current_value float,
    date_created DATETIME DEFAULT NOW(),
    updatedOn DATETIME DEFAULT NOW(),

    PRIMARY KEY(portfolio_id)
);

ALTER TABLE Portfolio AUTO_INCREMENT = 1000;
