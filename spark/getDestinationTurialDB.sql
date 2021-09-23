CREATE TEMPORARY VIEW jdbcTable
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:postgresql:dbserver",
  dbtable "source1.municipalities",
  user 'postgres',
  password 'postgres2'
)

INSERT INTO TABLE jdbcTable
SELECT * FROM resultTable

