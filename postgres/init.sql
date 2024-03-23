-- create role table
CREATE TABLE IF NOT EXISTS stream (
    tweet_created TIMESTAMP,
    "text" TEXT,
    polarity REAL
);

-- insert initial default roles
--INSERT INTO ROLE (id,name) VALUES (0,'USER'),(1,'ADMIN'),(2,'SLAVE'); 
