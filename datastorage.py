from confluent_kafka import Consumer
import json
import mysql.connector
from mysql.connector import errorcode

print('#DATA storage#\n')

n_statistics=4
n_prediction=5

###################
c = Consumer({
     'bootstrap.servers': 'localhost:29092',
     'group.id': 'mygroup',
     'auto.offset.reset': 'earliest'
})

print('Available topics to consume: ', c.list_topics().topics)

  # Subscription sul topic
c.subscribe(['promethuesdata'])


#---------------------------------DataBase------------------------#
try:
    print('\n#--------------------Connecting DB-------------#\n')
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="test_DSB",
        port=3306
    )
    print('#------------Connection to DB succed !------------#\n')
    cursordb = db.cursor()
    #------------------------------DB Cleaning------------------------#
    cursordb.execute("DELETE FROM metrics")
    cursordb.execute("DELETE FROM autocorrelation")
    cursordb.execute("DELETE FROM seasonability")
    cursordb.execute("DELETE FROM stationarity")
    cursordb.execute("DELETE FROM pred_max")
    cursordb.execute("DELETE FROM pred_min")
    cursordb.execute("DELETE FROM pred_mean")

    print('#------------Cleaning complete !------------#\n')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("ACCESS_DENIED_ERROR")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("BAD_DB")
    else:
        print(err)


def SQL_INSERT_1H(metric, msg): 
    metric = msg['Metric'] 
    max= msg['MAX_value'] 
    min= msg['min_value'] 
    mean= msg['mean_value'] 
    std = msg['std_value']
    

    
    sql = """INSERT INTO 1hMetrics (metric,max, min,mean) VALUES (%s,%s,%s);"""
    val = (metric,max,min,mean,std)
    #cursordb.execute(sql, val)
    #db.commit()

    list_aut = json.loads(msg['Autocorrelation'])
    for item in list_aut:    
        sql = """INSERT INTO 1hAutocorrelation (metric,value) VALUES (%s,%s);"""
        val = (metric,round(item, 4))        
        #cursordb.execute(sql, val)
        #db.commit()

    list_stat = json.loads(msg['Stationarity'])    
    sql = """INSERT INTO 1hStationarity (metric,value) VALUES (%s,%s);"""
    val = (metric,round(list_stat[0], 4),round(list_stat[1], 4),round(list_stat[2], 4),round(list_stat[3], 4),str(list_stat[4]),round(list_stat[5], 4))        
    #cursordb.execute(sql, val)
    #db.commit()



    for i in n_prediction:
        pmax=prediction_max1h[i]
        pmax=prediction_max1h[i]
        pmax=prediction_max1h[i]
        sql = """INSERT INTO 1hprediction (metric,max, min,mean,) VALUES (%s,%s,%s);"""
        val = (metric,max1h,min1h,mean1h,std1h,
        round(aut, 4),round(stat, 4), round(seas, 4), 
        pmax,)
        cursordb.execute(sql, val)
        db.commit()

print('#------------Pollin start!------------#')
while True:
    
    msg = c.poll(1.0)
    #print(msg)
    if msg is None:
        #print("...")
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    #print('Received message: {}'.format(msg.value().decode('utf-8')))
    timing = msg.key() 
    msg = json.loads(msg.value())
    print("")
    #print(timing)
    #print(msg)
    SQL_INSERT_1H(timing, msg) 
    

   

    

    """ key='CpuLoad'
    msg={
        "1h":{
            "Metrics": {
                "max":1,
                "min": 2,
                "mean": 3,
                "std": 4
            } ,
            "Statistics":{
                "Autocorrelation": [1,2,3,4,5,6,7,8,9],
                "Stazionarietà": [1,2,3,4,5,6,7,8,9],
                "Stagionalità": [1,2,3,4,5,6,7,8,9],
            } ,
            "Prediction" : {
                "Prediction_Max": [1,2,3,4,5],
                "Prediction_Min": [1,2,3,4,5],
                "Prediction_Mean": [1,2,3,4,5],
            }
        },
        "3h":{
            "Metrics": {
                "max":5,
                "min": 6,
                "mean": 7,
                "std": 8
            } ,
             "Metadati":{
                "Autocorrelation": [3,2,3,4,5,6,7,8,9],
                "Stazionarietà": [3,2,3,4,5,6,7,8,9],
                "Stagionalità": [3,2,3,4,5,6,7,8,9],
            } ,
            "Prediction" : {
                "Prediction_Max": [3,2,3,4,5],
                "Prediction_Min": [3,2,3,4,5],
                "Prediction_Mean": [3,2,3,4,5],
            }
        },
        "12h":{
            "Metrics": {
                "max":9,
                "min": 10,
                "mean": 11,
                "std": 12
            } ,
             "Metadati":{
                "Autocorrelation": [12,2,3,4,5,6,7,8,9],
                "Stazionarietà": [12,2,3,4,5,6,7,8,9],
                "Stagionalità": [12,2,3,4,5,6,7,8,9],
            } ,
            "Prediction" : {
                "Prediction_Max": [12,2,3,4,5],
                "Prediction_Min": [12,2,3,4,5],
                "Prediction_Mean": [12,2,3,4,5],
            }
        }
        
    } 

    data1H = msg['1h']    
    #
    
    

    data = msg
    print(data) """

    
c.close()
####################








# polling kafka
while True:
    print('#DATA storage#\n')
    msg = consumer.poll(1.0)
    if msg is None:
        print('#  ERROR: msg is void #\n')
        continue
    elif msg.error():
        print("consumer error: {}".format(msg.error()))
        continue
    else:
        print('#------------Pollin kafka ------------#\n')
        msg_key = msg.key() 
        msg_value = msg.value()
        data = json.loads(msg_value) 
        print(data);
         #--------------------------
        max = data['Metric']['max']
        min = data['Metric']['min']
        mean = data['Metric']['mean']
        std = data['Metric']['std']

    # Selezione dei dati ed invio al database:

        clientSQL_Metric(msg_key, max, min, mean, std, data['Metric']['duration'])
    #---------------------------
        autocorrelation = data['Metadati']['Autocorrelation']
        lista_autocorrelation = json.loads(autocorrelation)
        clientSQL_Autocorrelation(msg_key, lista_autocorrelation, data['Metadati']['duration'])
    #-----------------------------
        seasonability = data['Metadati']['Stagionalità']
        list_seasonal = json.loads(seasonability)
        clientSQL_Seasonability(msg_key, list_seasonal, data['Metadati']['duration'])
    #-------------------------------
        stationarity = data['Metadati']['Stazionarietà']
        list_stat = json.loads(stationarity)
        clientSQL_Stationarity(msg_key, list_stat, data['Metadati']['duration'])
    #-------------------------------
        prediction_max = data['Predizione']['Prediction_Max']
        prediction_min = data['Predizione']['Prediction_Mean']
        prediction_mean = data['Predizione']['Prediction_Min']

    # -------------------------------
        list_max = json.loads(prediction_max) # lista dei valori di massimo predetti
        if (len(list_max) == 0):
            pass
        else:
            clientSQL_Prediction_Max(msg_key, list_max, data['Predizione']['duration'])
            pass
    # -------------------------------
        list_min = json.loads(prediction_min)  # lista dei valori di minimo predetti
        if (len(list_min) == 0):
            pass
        else:
            clientSQL_Prediction_Min(msg_key, list_min, data['Predizione']['duration'])
            pass
    # -------------------------------
        list_mean = json.loads(prediction_mean)  # lista dei valori di media predetti
        if (len(list_mean) == 0):
            pass
        else:
            clientSQL_Prediction_Mean(msg_key, list_mean, data['Predizione']['duration'])
            pass
        print("All Saved in DB")

#consumer.close()
#db.close()

#Tabelle SQL
# """
# CREATE TABLE metrics ( 
# ID INT AUTO_INCREMENT, 
# metric varchar(255),
# max DOUBLE, 
# min DOUBLE, 
# mean DOUBLE, 
# dev_std DOUBLE, 
# duration varchar(255) 
# ,PRIMARY KEY (ID));
# CREATE TABLE autocorrelation (ID INT AUTO_INCREMENT, 
# metric varchar(255)
# ,value DOUBLE,
#  duration varchar(255)
# PRIMARY KEY(ID));
# CREATE TABLE seasonability (ID INT AUTO_INCREMENT, 
# metric varchar(255),
# value DOUBLE,
# duration varchar(255),
#  PRIMARY KEY(ID));
# CREATE TABLE stationarity (ID INT AUTO_INCREMENT, metric varchar(255),p_value DOUBLE,critical_values varchar(255),duration varchar(255), PRIMARY KEY(ID));
# CREATE TABLE prediction_mean (ID INT AUTO_INCREMENT, metric varchar(255), timestamp varchar(255), value varchar(255), duration varchar(255),PRIMARY KEY(ID) );
# CREATE TABLE prediction_min (ID INT AUTO_INCREMENT, metric varchar(255), timestamp varchar(255), value varchar(255), duration varchar(255),PRIMARY KEY(ID) );
# CREATE TABLE prediction_max (ID INT AUTO_INCREMENT, metric varchar(255), timestamp varchar(255), value varchar(255), duration varchar(255),PRIMARY KEY(ID) );
# """