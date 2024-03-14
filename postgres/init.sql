-- create role table
CREATE TABLE IF NOT EXISTS stream (
    id INT PRIMARY KEY,
    ts TIMESTAMP,
    polarity INT
);

-- insert initial default roles
--INSERT INTO ROLE (id,name) VALUES (0,'USER'),(1,'ADMIN'),(2,'SLAVE'); 
