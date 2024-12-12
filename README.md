# Redshift Datawarehouse Solution for Retail Data Analytics

## Overview
- In this project I've built redshift data warehouse solution for retail data analytics by using various features of aws redshift such as spectrum, federated queries,...
- The retail data has 6 different entities namely categories, departments, products, orders, order_items an customers.
- Categories and departments are the smallest possible entities, so I decided to create redshift managed tables and loaded the data into the tables using COPY commands.
- Orders and order_items are big entities, so data stays in s3 and glue meta data is used to procces in spectrum layer of redshift.
- Products and customers are from rds Mysql db. Used Federated queries to process them in redshift.

- I have designed this as an end to end solution:
  - creating redshift cluster, creating tables and external schemas for spectrum and federated queries
  - creating glue database and tables with data in s3
  - creating Mysql db in rds and creating tables and inserting data into the tables
  - and finally validated the schemas by running a join query
