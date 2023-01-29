## ![](https://images.squarespace-cdn.com/content/v1/60056c48dfad4a3649200fc0/1613294634908-3HTA3TR74HYYSNEIZSIJ/UniCT-Logo.jpg?format=1500w)

# DSBD project

---

### by Fabiola Marchi' e Matteo Terranova

> Indirizzo prometheus: http://15.160.61.227:29090

---

## Utilization Guide

`docker network create marchiterranova` --> to create network

`docker network ls` --> to verify network

`docker-compose up -d` --> in service directory to create service container (kafka, zookyper, SQL)

`docker-compose stop` --> stop container

`docker compose down --remove-orphans` --> to delete containers

`docker ps` --> to verify containerID & select SQLcontainerID

`docker exec -it <SQLcontainerID> bash` --> to execute DB

`mysql -u root -p test_DSB` --> insert DB psw: "root"

> Take care of -- **_database.sql_** -- file to populate db

`docker-compose up -d` --> in DSBproject directory to create containers

---

## List of API url

1. GET ETLdatapipeline to print all Kafka message send --> http://localhost:5000/all
2. GET ETLdatapipeline to print performace times of metrics, evaluation and prediction --> http://localhost:5000/performance
3. GET Dataretrival to print all DB content --> http://localhost:5050/all
4. To prin single table you can use name of table like: metrics, autocorrelation, stationarity, seasonability, prediction; also add name of metrics. See following examples:
   http://localhost:5050/metrics or http://localhost:5050/prediction/<name_of_metric>

> Read following paragraph after you can run this command

5. GET SLA_manager to print all SLA status --> http://localhost:5100/SLA_status

6. GET SLA_manager to print violations --> http://localhost:5100/show_Violation

7. GET SLA_manager to print predict violations splitted by timing --> http://localhost:5100/Violations_Number

8. GET SLA_manager to print predict violations --> http://localhost:5100/predict_Violations

9. GET SLA_manager to print predict violations splitted by name --> http://localhost:5100/predict_Violations&Name

---

## Execute following command to request POST on SLA manager

```bash
curl -i -X POST -H "Content-Type:application/json" -d "{"availableMem": [0, 94.33874],  "cpuLoad": [0, 1.5],"cpuTemp": [0, 38] ,"diskUsage": [0, 21.7735],"networkThroughput": [0, 0.01]}" "http://localhost:5002/SLA_Manager"
```

> Take care of -- **_SLApost.json_** -- file to modify and insert after <-d> option in curl cmd above
