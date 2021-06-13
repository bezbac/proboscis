DROP TABLE IF EXISTS users;

CREATE TABLE users (
    id      SERIAL PRIMARY KEY,
    name    TEXT NOT NULL
);

INSERT INTO users (name) VALUES ('Max');