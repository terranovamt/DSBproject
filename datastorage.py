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

    #------------------------SQL INSERT Predictions'-----------------------#
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

    #------------------------SQL INSERT Predictions'-----------------------#

    list_pmax =msg['Prediction_MAX']
    list_pmin =msg['Prediction_min']
    list_pmean =msg['Prediction_Mean']
    for i in range(n_prediction):
        
        sql = """INSERT INTO 12hPrediction (metric,max,min,mean) VALUES (%s,%s,%s,%s);"""
        val = (metric,list_pmax[i],list_pmin[i],list_pmean[i])
        cursordb.execute(sql, val)
        db.commit()

    print('#--------INSERT SQL_INSERT_12H DONE!-------#')
        
#------------------------Polling-----------------------#
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
db.close()
