![](https://images.squarespace-cdn.com/content/v1/60056c48dfad4a3649200fc0/1613294634908-3HTA3TR74HYYSNEIZSIJ/UniCT-Logo.jpg?format=1500w)
-------------------------------------------------------------
# DSBD project 
-------------------------------------------------------------
### by Fabiola Marchi' e Matteo Terranova

> Indirizzo prometheus: http://15.160.61.227:29090
-------------------------------------------------------------
## Utilization Guide

```docker network create marchiterranova``` --> to create network

```docker network ls``` --> to verify network

```docker-compose up -d``` --> in service directory to create service container (kafka, zookyper, SQL)

```docker-compose stop``` --> stop container

```docker compose down --remove-orphans``` --> to delete containers

```docker ps``` --> to verify containerID & select SQLcontainerID

```docker exec -it <SQLcontainerID> bash``` --> to execute DB

```mysql -u root -p test_DSB```  --> insert DB psw: "root"

>  Take care of -- ***database.sql*** -- file to populate db 

```docker-compose up -d``` --> in DSBproject directory to create containers

______________________________________________________________________

### Execute following command to request POST on SLA manager

```bash 
curl -i -X POST -H "Content-Type:application/json" -d "{"availableMem": [0, 94.33874],  "cpuLoad": [0, 1.5],"cpuTemp": [0, 38] ,"diskUsage": [0, 21.7735],"networkThroughput": [0, 0.01]}" "http://localhost:5002/SLA_Manager"
``` 

>  Take care of -- ***SLApost.json*** -- file to modify and insert after <-d> option in curl cmd above
