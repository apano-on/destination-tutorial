CREATE SCHEMA if not exists source1;

DROP TEMPORARY VIEW IF EXISTS jdbcTable;
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
    geometryPoint STRING,
    istat STRING
    );

INSERT INTO TABLE source1.municipalities
SELECT * FROM jdbcTable;


