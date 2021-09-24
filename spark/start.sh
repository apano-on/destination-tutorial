#!/bin/sh

start-master.sh \
	--host localhost \
	--port 7077 \
	--webui-port 8050

start-worker.sh \
	spark://localhost:7077 \
	--host localhost \
	--port 7077 \
	--webui-port 8051

# Based on https://sedona.apache.org/tutorial/sql-pure-sql/
# NOTE - Does not work because thriftserver does not seem to recognize this
#spark-sql \
#	--packages org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.1-incubating,org.apache.sedona:sedona-viz-3.0_2.12:1.0.1-incubating,org.datasyslab:geotools-wrapper:geotools-24.0 \
#	--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
#	--conf spark.kryo.registrator=org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator \
#	--conf spark.sql.extensions=org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions

spark-sql \
	--name SPARKSQL-INSERTIONS \
	--master spark://localhost:7077 \
	--deploy-mode client  \
	-f ./getDestinationTutorialDB.sql
	# -f ./getDestinationTutorialDB-solution.sql

start-thriftserver.sh \
	--name THRIFTSERVER \
	--master spark://localhost:7077 \
	--deploy-mode client \
	--hiveconf hive.server2.thrift.port=10000 \
	--hiveconf hive.server2.thrift.bind.host=0.0.0.0 \

tail -f /dev/null
