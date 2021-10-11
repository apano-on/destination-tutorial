CREATE SCHEMA if not exists source1;

DROP VIEW IF EXISTS jdbcTable;
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


DROP VIEW IF EXISTS jdbcTable;
CREATE TEMPORARY VIEW jdbcTable
 USING org.apache.spark.sql.jdbc
 OPTIONS (
   driver "org.postgresql.Driver",
   url "jdbc:postgresql://db:5432/",
   dbtable "source1.hospitality",
   user 'postgres',
   password 'postgres2'
 );
CREATE TABLE source1.hospitality (
                                     h_id STRING NOT NULL,
                                     name_en STRING NOT NULL,
                                     name_it STRING,
                                     name_de STRING NOT NULL,
                                     telephone STRING not NULL,
                                     email STRING NOT NULL,
                                     h_type STRING NOT NULL,
                                     latitude DOUBLE NOT NULL,
                                     longitude DOUBLE NOT NULL,
                                     altitude DOUBLE NOT NULL,
                                     category STRING NOT NULL,
                                     geometryPoint STRING,
                                     m_id STRING NOT NULL
);

INSERT INTO TABLE source1.hospitality
SELECT * FROM jdbcTable;


DROP VIEW IF EXISTS jdbcTable;
CREATE TEMPORARY VIEW jdbcTable
 USING org.apache.spark.sql.jdbc
 OPTIONS (
   driver "org.postgresql.Driver",
   url "jdbc:postgresql://db:5432/",
   dbtable "source1.rooms",
   user 'postgres',
   password 'postgres2'
 );
CREATE TABLE source1.rooms (
                               r_id STRING NOT NULL,
                               name_en STRING NOT NULL,
                               name_de STRING NOT NULL,
                               name_it STRING NOT NULL,
                               room_units INTEGER,
                               r_type STRING NOT NULL,
                               capacity INTEGER,
                               description_de STRING,
                               description_it STRING,
                               h_id STRING NOT NULL
);
INSERT INTO TABLE source1.rooms
SELECT * FROM jdbcTable;



CREATE SCHEMA if not exists source2;

DROP VIEW IF EXISTS jdbcTable;
CREATE TEMPORARY VIEW jdbcTable
 USING org.apache.spark.sql.jdbc
 OPTIONS (
   driver "org.postgresql.Driver",
   url "jdbc:postgresql://db:5432/",
   dbtable "source2.hotels",
   user 'postgres',
   password 'postgres2'
 );

CREATE TABLE source2.hotels (
                                id STRING NOT NULL,
                                english STRING NOT NULL,
                                italian STRING,
                                german STRING NOT NULL,
                                htype INTEGER,
                                lat DOUBLE NOT NULL,
                                long DOUBLE NOT NULL,
                                alt DOUBLE NOT NULL,
                                cat STRING NOT NULL,
                                mun INTEGER NOT NULL,
                                geom STRING
);

INSERT INTO TABLE source2.hotels
SELECT * FROM jdbcTable;



DROP VIEW IF EXISTS jdbcTable;
CREATE TEMPORARY VIEW jdbcTable
 USING org.apache.spark.sql.jdbc
 OPTIONS (
   driver "org.postgresql.Driver",
   url "jdbc:postgresql://db:5432/",
   dbtable "source2.accommodation",
   user 'postgres',
   password 'postgres2'
 );
CREATE TABLE source2.accommodation (
                                       id STRING NOT NULL,
                                       english_title STRING NOT NULL,
                                       german_title STRING NOT NULL,
                                       italian_title STRING NOT NULL,
                                       acco_type INTEGER NOT NULL,
                                       guest_nb INTEGER,
                                       german_description STRING,
                                       italian_description STRING,
                                       hotel STRING NOT NULL
);

INSERT INTO TABLE source2.accommodation
SELECT * FROM jdbcTable;



CREATE SCHEMA if not exists source3;

DROP VIEW IF EXISTS jdbcTable;
CREATE TEMPORARY VIEW jdbcTable
 USING org.apache.spark.sql.jdbc
 OPTIONS (
   driver "org.postgresql.Driver",
   url "jdbc:postgresql://db:5432/",
   dbtable "source3.weather_platforms",
   user 'postgres',
   password 'postgres2'
 );
CREATE TABLE source3.weather_platforms (
                                           id BIGINT NOT NULL,
                                           `name` varchar(255) NOT NULL,
                                           pointprojection STRING NOT NULL
);
INSERT INTO TABLE source3.weather_platforms
SELECT * FROM jdbcTable;



DROP VIEW IF EXISTS jdbcTable;
CREATE TEMPORARY VIEW jdbcTable
 USING org.apache.spark.sql.jdbc
 OPTIONS (
   driver "org.postgresql.Driver",
   url "jdbc:postgresql://db:5432/",
   dbtable "source3.measurement_types",
   user 'postgres',
   password 'postgres2'
 );
CREATE TABLE source3.measurement_types (
                                           `name` STRING NOT NULL,
                                           `unit` STRING NOT NULL,
                                           `description` STRING,
                                           `statisticalType` STRING
);
INSERT INTO TABLE source3.measurement_types
SELECT * FROM jdbcTable;


DROP VIEW IF EXISTS jdbcTable;
CREATE TEMPORARY VIEW jdbcTable
 USING org.apache.spark.sql.jdbc
 OPTIONS (
   driver "org.postgresql.Driver",
   url "jdbc:postgresql://db:5432/",
   dbtable "source3.weather_measurement",
   user 'postgres',
   password 'postgres2'
 );
CREATE TABLE source3.weather_measurement (
                                             id BIGINT NOT NULL,
                                             `period` INTEGER NOT NULL,
                                             `timestamp` timestamp NOT NULL,
                                             `name` STRING NOT NULL,
                                             double_value DOUBLE NOT NULL,
                                             platform_id BIGINT NOT NULL
);

INSERT INTO TABLE source3.weather_measurement
SELECT * FROM jdbcTable;

DROP VIEW IF EXISTS jdbcTable;

-- To maintain the statistics up-to-date, run ANALYZE TABLE after writing to the table.
-- The statistics/metadata info can be used by the cost optimizer
ANALYZE TABLE source1.municipalities COMPUTE STATISTICS;
ANALYZE TABLE source1.hospitality COMPUTE STATISTICS;
ANALYZE TABLE source1.rooms COMPUTE STATISTICS;
ANALYZE TABLE source2.hotels COMPUTE STATISTICS;
ANALYZE TABLE source2.accommodation COMPUTE STATISTICS;
ANALYZE TABLE source3.weather_platforms COMPUTE STATISTICS;
ANALYZE TABLE source3.measurement_types COMPUTE STATISTICS;
ANALYZE TABLE source3.weather_measurement COMPUTE STATISTICS;
