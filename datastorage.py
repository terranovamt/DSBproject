from confluent_kafka import Consumer
import json
import mysql.connector
from mysql.connector import errorcode

print('#DATA storage#\n')

###################
c = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'latest'
})

 # Subscription sul topic
c.subscribe(['promethuesdata'])


###############################################
# DATABASE
###############################################
try:
    print('#--------------------Connecting DB-------------#')
    db = mysql.connector.connect(
        host="mysqldb",
        user="root",
        password="root",
        database="test_DSB",
        port=3306
    )
    mtdb = db.cursor()
    print('#------------Connection to DB succed !------------#')
    #Clean all DB
    mtdb.execute("DELETE FROM metrics")
    mtdb.execute("DELETE FROM autocorrelation")
    mtdb.execute("DELETE FROM seasonability")
    mtdb.execute("DELETE FROM stationarity")
    mtdb.execute("DELETE FROM pred_max")
    mtdb.execute("DELETE FROM pred_min")
    mtdb.execute("DELETE FROM pred_mean")

    print('#------------Clean complete !------------#\n')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("ACCESS_DENIED_ERROR")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("BAD_DB")
    else:
        print(err)


while True:
    
    msg = c.poll(1.0)
    #print(msg)
    if msg is None:
        #print("msg is none")
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

    key = msg.key() 
    msg = msg.value()
    print("\n\nCIAOO\n\n")
    data = json.loads(msg) 
    print(data)

c.close()
####################








# # polling kafka
# while True:
#     print('#DATA storage#\n')
#     msg = consumer.poll(1.0)
#     if msg is None:
#         print('#  ERROR: msg is void #\n')
#         continue
#     elif msg.error():
#         print("consumer error: {}".format(msg.error()))
#         continue
#     else:
#         print('#------------Pollin kafka ------------#\n')
#         msg_key = msg.key() 
#         msg_value = msg.value()
#         data = json.loads(msg_value) 
#         print(data);
#          #--------------------------
#         max = data['Metric']['max']
#         min = data['Metric']['min']
#         mean = data['Metric']['mean']
#         std = data['Metric']['std']

#     # Selezione dei dati ed invio al database:

#         clientSQL_Metric(msg_key, max, min, mean, std, data['Metric']['duration'])
#     #---------------------------
#         autocorrelation = data['Metadati']['Autocorrelation']
#         lista_autocorrelation = json.loads(autocorrelation)
#         clientSQL_Autocorrelation(msg_key, lista_autocorrelation, data['Metadati']['duration'])
#     #-----------------------------
#         seasonability = data['Metadati']['Stagionalità']
#         list_seasonal = json.loads(seasonability)
#         clientSQL_Seasonability(msg_key, list_seasonal, data['Metadati']['duration'])
#     #-------------------------------
#         stationarity = data['Metadati']['Stazionarietà']
#         list_stat = json.loads(stationarity)
#         clientSQL_Stationarity(msg_key, list_stat, data['Metadati']['duration'])
#     #-------------------------------
#         prediction_max = data['Predizione']['Prediction_Max']
#         prediction_min = data['Predizione']['Prediction_Mean']
#         prediction_mean = data['Predizione']['Prediction_Min']

#     # -------------------------------
#         list_max = json.loads(prediction_max) # lista dei valori di massimo predetti
#         if (len(list_max) == 0):
#             pass
#         else:
#             clientSQL_Prediction_Max(msg_key, list_max, data['Predizione']['duration'])
#             pass
#     # -------------------------------
#         list_min = json.loads(prediction_min)  # lista dei valori di minimo predetti
#         if (len(list_min) == 0):
#             pass
#         else:
#             clientSQL_Prediction_Min(msg_key, list_min, data['Predizione']['duration'])
#             pass
#     # -------------------------------
#         list_mean = json.loads(prediction_mean)  # lista dei valori di media predetti
#         if (len(list_mean) == 0):
#             pass
#         else:
#             clientSQL_Prediction_Mean(msg_key, list_mean, data['Predizione']['duration'])
#             pass
#         print("All Saved in DB")

#consumer.close()
#db.close()

#Tabelle SQL
# """
# CREATE TABLE metrics ( ID INT AUTO_INCREMENT, metric varchar(255),max DOUBLE, min DOUBLE, mean DOUBLE, dev_std DOUBLE, duration varchar(255) ,PRIMARY KEY (ID));
# CREATE TABLE autocorrelation (ID INT AUTO_INCREMENT, metric varchar(255),value DOUBLE, duration varchar(255),PRIMARY KEY(ID));
# CREATE TABLE seasonability (ID INT AUTO_INCREMENT, metric varchar(255),value DOUBLE,duration varchar(255), PRIMARY KEY(ID));
# CREATE TABLE stationarity (ID INT AUTO_INCREMENT, metric varchar(255),p_value DOUBLE,critical_values varchar(255),duration varchar(255), PRIMARY KEY(ID));
# CREATE TABLE prediction_mean (ID INT AUTO_INCREMENT, metric varchar(255), timestamp varchar(255), value varchar(255), duration varchar(255),PRIMARY KEY(ID) );
# CREATE TABLE prediction_min (ID INT AUTO_INCREMENT, metric varchar(255), timestamp varchar(255), value varchar(255), duration varchar(255),PRIMARY KEY(ID) );
# CREATE TABLE prediction_max (ID INT AUTO_INCREMENT, metric varchar(255), timestamp varchar(255), value varchar(255), duration varchar(255),PRIMARY KEY(ID) );
# """