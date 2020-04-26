-- TABLE FOR ALL TICKERS IN OUR DB
DROP TABLE tickers;

CREATE TABLE tickers(
    ticker_name VARCHAR(5),
    -- company_name VARCHAR(50),
    sector_name VARCHAR(50),
    industry_name VARCHAR(50),
    industry_group_name VARCHAR(50),
    sub_industry_name VARCHAR(50),

    PRIMARY KEY(ticker_name)   

);
