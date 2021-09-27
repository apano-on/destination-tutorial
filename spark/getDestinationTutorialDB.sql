CREATE SCHEMA if not exists source1;

DROP VIEW IF EXISTS jdbcTable;
-- We might consider letting Spark infer the schema and save some time
--CREATE TABLE source1.municipalities
CREATE TEMPORARY VIEW jdbcTable
 USING org.apache.spark.sql.jdbc
 OPTIONS (
   driver "org.postgresql.Driver",
   url "jdbc:postgresql://db:5432/",
   dbtable "source1.municipalities",
   user 'postgres',
   password 'postgres2'
 );

CREATE TABLE source1.municipalities(
    m_id STRING NOT NULL,
    istat STRING NOT NULL,
    name_en STRING NOT NULL,
    name_it STRING NOT NULL,
    name_de STRING NOT NULL,
   `population` INTEGER,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    altitude DOUBLE NOT NULL,
    geometryPoint STRING
    );

INSERT INTO TABLE source1.municipalities
SELECT * FROM jdbcTable;

-- Approach fails because sedona functions are temporary
-- But temporary tables cannot be persisted, it seems the spark shell session
-- ends once we run the thriftserver, therefore we no longer have any access
--CREATE TEMPORARY VIEW municipalities AS
--SELECT *,
--       ST_GEOMFROMWKB(geometryPoint) As geom
--FROM source1.municipalities;