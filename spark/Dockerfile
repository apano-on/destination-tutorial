FROM debian

RUN apt update
RUN apt install wget default-jdk scala git python3 -y
RUN apt-get install -y curl

WORKDIR /tmp

RUN wget http://downloads.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz
RUN tar xvf spark-*
RUN mv spark-3.1.2-bin-hadoop3.2 /opt/spark

RUN apt install procps -y

ENV SPARK_HOME=/opt/spark
ENV PATH=${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin
ENV PYSPARK_PYTHON=/usr/bin/python3

WORKDIR /home

COPY start.sh start.sh
COPY getDestinationTurialDB.sql getDesinationTutorialDB.sql

RUN chmod +x start.sh

#NOTE: In the future the jars can be retrieved from maven / they can either be downloaded manually or pulled from maven at build time
RUN cd ${SPARK_HOME}/jars && \
wget "https://search.maven.org/remotecontent?filepath=org/apache/sedona/sedona-python-adapter-3.0_2.12/1.0.1-incubating/sedona-python-adapter-3.0_2.12-1.0.1-incubating.jar" && \
wget "https://search.maven.org/remotecontent?filepath=org/apache/sedona/sedona-viz-3.0_2.12/1.0.1-incubating/sedona-viz-3.0_2.12-1.0.1-incubating.jar" && \
wget "https://search.maven.org/remotecontent?filepath=org/datasyslab/geotools-wrapper/geotools-24.0/geotools-wrapper-geotools-24.0.jar"
#COPY sedona-python-adapter-3.0_2.12-1.0.1-incubating.jar /opt/spark/jars/sedona-python-adapter-3.0_2.12-1.0.1-incubating.jar
#COPY sedona-viz-3.0_2.12-1.0.1-incubating.jar /opt/spark/jars/sedona-viz-3.0_2.12-1.0.1-incubating.jar
#COPY geotools-wrapper-geotools-24.0.jar /opt/spark/jars/geotools-wrapper-geotools-24.0.jar
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

EXPOSE 8050
EXPOSE 8051
EXPOSE 10000

CMD [ "./start.sh" ]