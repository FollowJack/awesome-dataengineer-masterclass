# Retail analytics

This project aims at generating insights around the daily & monthly transactions which will help the business to execute campaigns for Cross-sell and Up-sell. The reporting team's insights can also be used to generate dashboards and present the outcome in a visually appealing format.

The retail analytics project consists of retail data points including customers, categories/departments, products, and transactional data.

Data is imported from the RDBMS into the Hive data warehouse using Apache Sqoop.
Business logic is applied and this data is processed using Apache Spark to generate multiple insights. The final output is stored back to Hive, consumed by the campaign/marketing/reporting teams. The entire process can be automated using Oozie/Airflow.

## Setup

Follow this [article](https://blog.clairvoyantsoft.com/cloduera-quickstart-vm-using-docker-on-mac-2308acd196f2) to download the docker setup.

## Run

Run `docker ps` to find main container running and grep the container id.

Copy the local scripts into the home directory of your container via:

```
docker cp scripts [container_id]:/home
```

Then connect to the container via
`docker exec -it [container_id] /bin/bash`

Follow the steps along.

1. Update our existing MYSQL database for current dates.
2. Import data into hive with scoop.
3. Generating insights using transformations and aggregations for analytics team with spark and store the output back to hive.

### Update existing MYSQL database for current dates

Inside the container connect with the database. Password is 'cloudera'

```
mysql -u root -p
```

Run `show databases` in order to see if the "retail" db is available.

Run the script `/home/scripts/mysql_date_update_sql.sh`.
The last updated date will be displayed in the end.

### Import data into hive with scoop

Create database in hive, import tables and setup partitioning via the shell script

```
sh hive_scoop_import.sh
```

### Generating insights using transformations and aggregations for analytics team with spark and store the output back to hive.

Create database in hive, import tables and setup partitioning via the shell script

```

sh spark_retail_processing.sh
```

Finally analysts can use the results to display information.

[Department wise total sales](./docs/department_total_sales.png)
