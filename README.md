# DSBD project 
### by Fabiola Marchi' e Matteo Terranova

## Utilization Guide


### Indirizzo prometheus: http://15.160.61.227:29090

```docker network create marchiterranova``` --> to create network

```docker network ls``` --> to verify network

```docker-compose up -d``` --> in service directory to create service container (kafka, zookyper, SQL)

```docker-compose stop``` --> stop container

```docker compose down --remove-orphans``` --> to delete containers

```docker ps``` --> to verify containerID & select SQLcontainerID

```docker exec -it <SQLcontainerID> bash``` --> to execute DB

```mysql -u root -p test_DSB```  --> insert DB psw: "root"

>  Take care of *database.sql* file to populate db 

```docker-compose up -d``` --> in DSBproject directory to create containers
______________________________________________________________________

### Execute following command to request POST on SLA manager

``` curl -i -X POST -H "Content-Type:application/json" -d "{"availableMem": [0, 94.33874],  "cpuLoad": [0, 1.5],"cpuTemp": [0, 38] ,"diskUsage": [0, 21.7735],"networkThroughput": [0, 0.01]}" "http://localhost:5002/SLA_Manager"``` 

