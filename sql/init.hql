CREATE DATABASE IF NOT EXISTS DWHNYCTAXI;
USE DWHNYCTAXI;

CREATE EXTERNAL TABLE IF NOT EXISTS DWHNYCTAXI.dimension_mois (
    id_dim_mois INT,
    mois INT,
    PRIMARY KEY(id_dim_mois) DISABLE NOVALIDATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/DWHNYCTAXI.db/dimension_mois';

CREATE EXTERNAL TABLE IF NOT EXISTS DWHNYCTAXI.dimension_trajet (
    id_dim_trajet INT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    PRIMARY KEY(id_dim_trajet) DISABLE NOVALIDATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/DWHNYCTAXI.db/dimension_trajet';

CREATE EXTERNAL TABLE IF NOT EXISTS DWHNYCTAXI.fait_nombre_trajet (
    id_fait_nombre_trajet INT,
    id_dim_mois INT,
    id_dim_trajet INT,
    PRIMARY KEY(id_fait_nombre_trajet) DISABLE NOVALIDATE,
    CONSTRAINT fk_mois FOREIGN KEY (id_dim_mois) REFERENCES DWHNYCTAXI.dimension_mois(id_dim_mois) DISABLE NOVALIDATE,
    CONSTRAINT fk_trajet FOREIGN KEY (id_dim_trajet) REFERENCES DWHNYCTAXI.dimension_trajet(id_dim_trajet) DISABLE NOVALIDATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/DWHNYCTAXI.db/fait_nombre_trajet';

