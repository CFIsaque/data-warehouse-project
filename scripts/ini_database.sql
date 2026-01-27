--Bronze

CREATE SCHEMA IF NOT EXISTS bronze
OPTIONS (
  location = "US"
);

--Prata
CREATE SCHEMA IF NOT EXISTS silver
OPTIONS (
  location = "US"
);

CREATE SCHEMA IF NOT EXISTS gold
OPTIONS (
  location = "US"
);
