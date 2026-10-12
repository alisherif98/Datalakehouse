-- docker exec -it trino trino --catalog iceberg
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SELECT * FROM iceberg.lab.smoke;
SELECT * FROM iceberg.lab."smoke$snapshots";
SELECT * FROM iceberg.lab."smoke$files";
-- Trino as an ETL source: TPC-H sample data -> Iceberg
CREATE TABLE IF NOT EXISTS iceberg.lab.orders_tpch AS SELECT * FROM tpch.tiny.orders;
