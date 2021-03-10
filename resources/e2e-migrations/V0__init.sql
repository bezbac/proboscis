CREATE TABLE person (
    id      SERIAL PRIMARY KEY,
    name    TEXT NOT NULL
);

INSERT INTO person (name) VALUES ('Max');