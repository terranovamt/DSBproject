from confluent_kafka import Consumer
import json
import mysql.connector
from mysql.connector import errorcode

print('#DATA storage#\n')

n_statistics=4
n_prediction=5

#----------------------Consumer Configuration------------------------#
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
    cursordb.execute("DELETE FROM 1hMetrics")
    cursordb.execute("DELETE FROM 1hAutocorrelation")
    cursordb.execute("DELETE FROM 1hStationarity")
    cursordb.execute("DELETE FROM 1hSeasonability")
    cursordb.execute("DELETE FROM 1hPrediction")
    cursordb.execute("DELETE FROM 3hMetrics")
    cursordb.execute("DELETE FROM 3hAutocorrelation")
    cursordb.execute("DELETE FROM 3hStationarity")
    cursordb.execute("DELETE FROM 3hSeasonability")
    cursordb.execute("DELETE FROM 3hPrediction")
    cursordb.execute("DELETE FROM 12hMetrics")
    cursordb.execute("DELETE FROM 12hAutocorrelation")
    cursordb.execute("DELETE FROM 12hStationarity")
    cursordb.execute("DELETE FROM 12hSeasonability")
    cursordb.execute("DELETE FROM 12hPrediction")

    print('#------------Cleaning complete !------------#\n')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("ACCESS_DENIED_ERROR")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("BAD_DB")
    else:
        print(err)
#------------------------------SQL INSERT------------------------------#

def SQL_INSERT_1H(metric, msg): 
    metric = msg['Metric'] 
    max= msg['MAX_value'] 
    min= msg['min_value'] 
    mean= msg['mean_value'] 
    std = msg['std_value']
     
    sql = """INSERT INTO 1hMetrics (metric,max,min,mean,std) VALUES (%s,%s,%s,%s,%s);"""
    val = (metric,max,min,mean,std)
    cursordb.execute(sql, val)
    db.commit()

    #-----------------------SQL INSERT Autocorrelation----------------------#
    list_aut = json.loads(msg['Autocorrelation'])
    for item in list_aut:    
        sql = """INSERT INTO 1hAutocorrelation (metric,value) VALUES (%s,%s);"""
        val = (metric,round(item, 4))        
        cursordb.execute(sql, val)
        db.commit()

    #------------------------SQL INSERT Stationarity------------------------#
    list_stat = json.loads(msg['Stationarity'])    
    sql = """INSERT INTO 1hStationarity (metric,adf,pvalue,usedlag,nobs,criticalvalues,icbest) VALUES (%s,%s,%s,%s,%s,%s,%s);"""
    val = (metric,round(list_stat[0], 4),round(list_stat[1], 4),round(list_stat[2], 4),round(list_stat[3], 4),str(list_stat[4]),round(list_stat[5], 4))        
    cursordb.execute(sql, val)
    db.commit()

    #------------------------SQL INSERT Seasonability-----------------------#
    list_seas = json.loads(msg['Seasonability'])
    for item in list_seas:    
        sql = """INSERT INTO 1hSeasonability (metric,value) VALUES (%s,%s);"""
        val = (metric,round(item, 4))        
        cursordb.execute(sql, val)
        db.commit()


    list_pmax =msg['Prediction_MAX']
    list_pmin =msg['Prediction_min']
    list_pmean =msg['Prediction_Mean']
    for i in range(n_prediction):
        
        sql = """INSERT INTO 1hPrediction (metric,max,min,mean) VALUES (%s,%s,%s,%s);"""
        val = (metric,list_pmax[i],list_pmin[i],list_pmean[i])
        cursordb.execute(sql, val)
        db.commit()

    print('#--------INSERT SQL_INSERT_1H DONE!-------#')

def SQL_INSERT_3H(metric, msg): 
    metric = msg['Metric'] 
    max= msg['MAX_value'] 
    min= msg['min_value'] 
    mean= msg['mean_value'] 
    std = msg['std_value']
        
    sql = """INSERT INTO 3hMetrics (metric,max,min,mean,std) VALUES (%s,%s,%s,%s,%s);"""
    val = (metric,max,min,mean,std)
    cursordb.execute(sql, val)
    db.commit()

    #-----------------------SQL INSERT Autocorrelation----------------------#
    list_aut = json.loads(msg['Autocorrelation'])
    for item in list_aut:    
        sql = """INSERT INTO 3hAutocorrelation (metric,value) VALUES (%s,%s);"""
        val = (metric,round(item, 4))        
        cursordb.execute(sql, val)
        db.commit()

    #------------------------SQL INSERT Stationarity------------------------#
    list_stat = json.loads(msg['Stationarity'])    
    sql = """INSERT INTO 3hStationarity (metric,adf,pvalue,usedlag,nobs,criticalvalues,icbest) VALUES (%s,%s,%s,%s,%s,%s,%s);"""
    val = (metric,round(list_stat[0], 4),round(list_stat[1], 4),round(list_stat[2], 4),round(list_stat[3], 4),str(list_stat[4]),round(list_stat[5], 4))        
    cursordb.execute(sql, val)
    db.commit()

    #------------------------SQL INSERT Seasonability-----------------------#
    list_seas = json.loads(msg['Seasonability'])
    for item in list_seas:    
        sql = """INSERT INTO 3hSeasonability (metric,value) VALUES (%s,%s);"""
        val = (metric,round(item, 4))        
        cursordb.execute(sql, val)
        db.commit()


    list_pmax =msg['Prediction_MAX']
    list_pmin =msg['Prediction_min']
    list_pmean =msg['Prediction_Mean']
    for i in range(n_prediction):
        
        sql = """INSERT INTO 3hPrediction (metric,max,min,mean) VALUES (%s,%s,%s,%s);"""
        val = (metric,list_pmax[i],list_pmin[i],list_pmean[i])
        cursordb.execute(sql, val)
        db.commit()

    print('#--------INSERT SQL_INSERT_3H DONE!-------#')


def SQL_INSERT_12H(metric, msg): 
    metric = msg['Metric'] 
    max= msg['MAX_value'] 
    min= msg['min_value'] 
    mean= msg['mean_value'] 
    std = msg['std_value']
        
    sql = """INSERT INTO 12hMetrics (metric,max,min,mean,std) VALUES (%s,%s,%s,%s,%s);"""
    val = (metric,max,min,mean,std)
    cursordb.execute(sql, val)
    db.commit()

    #-----------------------SQL INSERT Autocorrelation----------------------#
    list_aut = json.loads(msg['Autocorrelation'])
    for item in list_aut:    
        sql = """INSERT INTO 12hAutocorrelation (metric,value) VALUES (%s,%s);"""
        val = (metric,round(item, 4))        
        cursordb.execute(sql, val)
        db.commit()

    #------------------------SQL INSERT Stationarity------------------------#
    list_stat = json.loads(msg['Stationarity'])    
    sql = """INSERT INTO 12hStationarity (metric,adf,pvalue,usedlag,nobs,criticalvalues,icbest) VALUES (%s,%s,%s,%s,%s,%s,%s);"""
    val = (metric,round(list_stat[0], 4),round(list_stat[1], 4),round(list_stat[2], 4),round(list_stat[3], 4),str(list_stat[4]),round(list_stat[5], 4))        
    cursordb.execute(sql, val)
    db.commit()

    #------------------------SQL INSERT Seasonability-----------------------#
    list_seas = json.loads(msg['Seasonability'])
    for item in list_seas:    
        sql = """INSERT INTO 12hSeasonability (metric,value) VALUES (%s,%s);"""
        val = (metric,round(item, 4))        
        cursordb.execute(sql, val)
        db.commit()


    list_pmax =msg['Prediction_MAX']
    list_pmin =msg['Prediction_min']
    list_pmean =msg['Prediction_Mean']
    for i in range(n_prediction):
        
        sql = """INSERT INTO 12hPrediction (metric,max,min,mean) VALUES (%s,%s,%s,%s);"""
        val = (metric,list_pmax[i],list_pmin[i],list_pmean[i])
        cursordb.execute(sql, val)
        db.commit()

    print('#--------INSERT SQL_INSERT_12H DONE!-------#')
        

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
    t=str(timing)
    msg = json.loads(msg.value())
    print("")
    #print(timing)
    #print(msg)
    if str(timing) == "b'1h'":
        SQL_INSERT_1H(timing, msg) 
    if str(timing) == "b'3h'":
        SQL_INSERT_3H(timing, msg) 
    if str(timing) == "b'12h'":
        SQL_INSERT_12H(timing, msg) 
    

   

    

    

    
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
# 
"""INSERT INTO 1hMetrics (metric,max,min,mean,std) VALUES (%s,%s,%s,%s,%s);"""
"""INSERT INTO 1hAutocorrelation (metric,value) VALUES (%s,%s);"""
"""INSERT INTO 1hStationarity (metric,adf,pvalue,usedlag,nobs,criticalvalues,icbest) VALUES (%s,%s,%s,%s,%s,%s,%s);"""
"""INSERT INTO 1hPrediction (metric,max,min,mean) VALUES (%s,%s,%s);"""
"""INSERT INTO 1hSeasonability (metric,value) VALUES (%s,%s);"""
#CREATE TABLE 1hMetrics (ID INT AUTO_INCREMENT, metric varchar(255), max DOUBLE,min DOUBLE,mean DOUBLE,std DOUBLE,PRIMARY KEY(ID));
#CREATE TABLE 1hAutocorrelation (ID INT AUTO_INCREMENT, metric varchar(255),value DOUBLE,PRIMARY KEY(ID));
#CREATE TABLE 1hStationarity (ID INT AUTO_INCREMENT, metric varchar(255),adf DOUBLE,pvalue DOUBLE,usedlag DOUBLE,nobs DOUBLE,criticalvalues varchar(255),icbest DOUBLE,PRIMARY KEY(ID));
#CREATE TABLE 1hSeasonability (ID INT AUTO_INCREMENT, metric varchar(255),value DOUBLE,PRIMARY KEY(ID));
#CREATE TABLE 1hPrediction (ID INT AUTO_INCREMENT, metric varchar(255), max DOUBLE,min DOUBLE,mean DOUBLE,PRIMARY KEY(ID));
## 
# """