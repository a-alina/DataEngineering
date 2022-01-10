# Data Engineering course project
-----

## Project description
-----

The main goal of the course project is to design and implement data pipelines using Apache Airflow. 
The data for the project was scraped from KnowYourMeme.

## Repository structure
-----
```
- airflow                                   <- contains a Dockerfile
    - Dockerfile                            <- py2neo and pickle5 configuration
- dags                                      <- the main source of code
    - cleaning_dag.py                       <- script that contains cleaning pipline dag tasks
    - python_tasks.py                       <- script that contains cleaning dag python tasks
    - injection_dag.py                      <- script that contains injection pipline dag tasks
    - python_tasks_injection.py             <- script that contains injection dag python tasks
    - cypher_dag.py                         <- script that contains cypher pipline dag tasks
    - python_tasks_cypher.py                <- script that contains cypher dag python tasks
- docker-compose.yaml                       <- airflow configuration

```
