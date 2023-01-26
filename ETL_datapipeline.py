from prometheus_api_client import PrometheusConnect, MetricsList, MetricSnapshotDataFrame, MetricRangeDataFrame
from prometheus_api_client.utils import parse_datetime
from confluent_kafka import Producer
from flask import Flask, jsonify, request
from datetime import timedelta
from statsmodels.tsa.stattools import acf, adfuller, kpss
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.holtwinters import ExponentialSmoothing
import json
import time
import csv
import sys

#import openpyxl
import pandas as pd

broker = "localhost:29092"
topic = "promethuesdata"
conf = {'bootstrap.servers': broker}
p = Producer(**conf)
def delivery_callback(err, msg):
    if err:
        print('%% Message failed delivery: %s\n' % err)
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        print('%% Message delivered to %s [%d] @ %d\n' % (msg.topic(), msg.partition(), msg.offset()))
        sys.stderr.write('%% Message delivered to %s partition [%d] @ %d\n' %
                         (msg.topic(), msg.partition(), msg.offset()))

#try:
#    producer = Producer(**configuration)
#    print('Producer Connected')
#except KafkaExec as error:
#    print('Error: ', error)

print('#--------------------Connecting-------------#\n')
print('                     ..........              \n')
print('                     ..........              \n')
print('                     ..........              \n')
prom = PrometheusConnect(url="http://15.160.61.227:29090", disable_ssl=True)
print('#------------Connection succed !------------#\n')
#print('#                     Now you can get on Prometheus all metrics you want ;)                     #\n')
#--------------------------------------------FIRST-----------------------------------------------------#
#----------------------------------------PROMETHEUS' QUERY---------------------------------------------#
time_to_evaluate = ['3h', '1h']
metrics_to_evaluate = ['cpuLoad', 'cpuTemp', 'diskUsage', 'availableMem', 'networkThroughput'] #
end_time = parse_datetime("now")
chunk_size = timedelta(minutes=10)
label_config = {'nodeName': 'sv122','job': 'summary'} 
counter = 0



for metric in metrics_to_evaluate: 
   

    for timing in time_to_evaluate:    
        minValues_list = []
        maxValues_list = []
        meanValues_list = []
        stdValues_list = []
        performance_list = [] 
        kafka_message = []

        start_time = parse_datetime(timing)  
        t0_metrics = time.time() 
        metric_data = prom.get_metric_range_data(
            metric_name=metric,
            label_config=label_config,
            start_time=start_time,
            end_time=end_time,
            chunk_size=chunk_size,
        )
        metric_df = MetricRangeDataFrame(metric_data) # Creazione del data frame
        #print('METRIC_DF -> ', metric_df)
        #metric_object_list = MetricsList(metric_data)
        #--------------------------Evaluation of MAX, min, mean and std_dev----------------------------#
        print("Evaluation of MAX, min, mean and std_dev is on going")
        #print('                       ..........                    \n')
        #print('                       ..........                    \n')
        max_value = round(metric_df['value'].max(), 2)
        min_value = round(metric_df['value'].min(), 2)
        mean_value = round(metric_df['value'].mean(), 2)
        std_value = round(metric_df['value'].std(), 2)
        t1_metrics = time.time()
        #print("\n               Monitoring done!            \n")
        #print("---------------->MAX value: ", max_value)
        #print("---------------->min value: ", min_value)
        #print("---------------->MEAN value: ", mean_value)
        #print("---------------->STD_dev value: ", std_value)
        #print('                                           \n\n')
        minValues_list.append(min_value) 
        maxValues_list.append(max_value) 
        meanValues_list.append(mean_value)
        stdValues_list.append(std_value) 
        #-----------------------------------Packing message for Kafka (1/4)-----------------------------#
        #kafka_message.append(minValues_list)
        #kafka_message.append(maxValues_list)
        #kafka_message.append(meanValues_list)
        #kafka_message.append(minValues_list)
        #-----------------------------------------------------------------------------------------------#
        #-----------------Evaluation of AutoCorrelation, Stationarity & Seasonability-------------------#
        t0_evaluation = time.time()
        print("Evaluation of AutoCorrelation is on going ...")
        autocorrelation = acf(metric_df['value']) 
        autocorrelation_list = autocorrelation.tolist()
        del autocorrelation_list[0]
        #-----------------------------------Packing message for Kafka (2/4)-----------------------------#
        #kafka_message.append(autocorrelation_list)
        #-----------------------------------------------------------------------------------------------#
        # for autocorrelation_i in autocorrelation_list:
        #    print('AUTOCORRELATION ----> ', autocorrelation_i)

        print("Evaluation of Stationariety is on going ...   ")
        stationarity = adfuller(metric_df['value'],autolag='AIC') 
       # print('STATIONARITY ----> ', stationarity)
        #-----------------------------------Packing message for Kafka (3/4)-----------------------------#
        #kafka_message.append(stationarity)
        #-----------------------------------------------------------------------------------------------#
        print("Evaluation of Seasonability is on going ...   ")
        seasonability = seasonal_decompose(metric_df['value'], model='additive', period=10)
        seasonability_list = seasonability.seasonal.tolist() 
        #-----------------------------------Packing message for Kafka (4/4)-----------------------------#
        #kafka_message.append(seasonability_list)
        #-----------------------------------------------------------------------------------------------#
        t1_evaluation = time.time()
        # for seasonability_i in seasonability_list:
        #     print('SEASONABILITY ----> ', seasonability_i)

        #-------------------------------Prediction of each metric--------------------------------------#
        #---------->RESAMPLING
        #print("Prediction process is started now            \n")
        #print('                     ..........              \n')
        #print('                     ..........              \n')
        #print("RESAMPLING is on going ...                   \n")
        t0_prediction = time.time()
        MAX_resampling = metric_df['value'].resample(rule='2T').max()
        #print(" MAX RESAMPLING ---> \n", MAX_resampling)
        min_resampling = metric_df['value'].resample(rule='2T').min()
        #print(" MIN RESAMPLING ---> \n", min_resampling)
        mean_resampling = metric_df['value'].resample(rule='2T').mean()   
        #print(" MEAN RESAMPLING ---> \n", mean_resampling) 
        
        #---------->PREDICTION
        #print('                     ..........              \n')
        #print('                     ..........              \n')
        print("PREDICTION is on going ...                   ")
        tsmodel_MAX = ExponentialSmoothing(MAX_resampling, trend='add', seasonal='add',seasonal_periods=5).fit()
        tsmodel_min = ExponentialSmoothing(min_resampling, trend='add', seasonal='add',seasonal_periods=5).fit()
        tsmodel_mean = ExponentialSmoothing(mean_resampling, trend='add', seasonal='add',seasonal_periods=5).fit() 
        
        prediction_max = tsmodel_MAX.forecast(5) 
        #print("Prediction of MAX ---> \n", prediction_max)
        prediction_min = tsmodel_min.forecast(5)  
        #print("Prediction of min ---> \n", prediction_min)  
        prediction_mean = tsmodel_mean.forecast(5)        
        #print("Prediction of mean ---> \n", prediction_mean) 
        t1_prediction = time.time()  
        #---------------------------Saving metrics computation time on API/performance------------------------------#
        monitoring_metrics = {
            "Metrics":{
                "Metric Name": metric,
                "Timing": timing,
                "Computation Time [s]" : round((t1_metrics - t0_metrics), 2)
                            
            }
        }

        monitoring_evaluation = {
            "Evalution_Values":{
                "Metric Name": metric,
                "Timing": timing,
                "Computation Time [s]" : round((t1_evaluation - t0_evaluation), 2)
                    
            }
        }

        monitoring_prediction = {
            "Prediction_values":{
                "Metric Name": metric,
                "Timing": timing,
                "Computation Time [s]" : round((t1_prediction - t0_prediction), 2)
            
            }
        }
        performance_list.append(monitoring_metrics)
        performance_list.append(monitoring_evaluation)
        performance_list.append(monitoring_prediction)
        with open('Performance.log', 'a') as f:
            f.write(str(monitoring_metrics)) 
            f.write('\n')
            f.write(str(monitoring_evaluation))
            f.write('\n')
            f.write(str(monitoring_prediction))   
            f.write('\n')    
                
        counter += 1
        print("\nCOUTER ---> ", counter) 

    #     #--------------------------------Creating JSON to pack kafka message-------------------------#
       

        msg = {
                "Metric": metric,
                "MAX_value":max_value,
                "min_value": min_value,
                "mean_value": mean_value,
                "std_value": std_value,                
                "Autocorrelation": json.dumps(autocorrelation_list),
                "Stationarity": json.dumps(stationarity),
                "Seasonability": json.dumps(seasonability_list)
                } #,
                # "Prediction" : {
                #     "Prediction_MAX": prediction_max,
                #     "Prediction_min": prediction_min,
                #     "Prediction_Mean": prediction_mean
                # }                

        #print('msg_kafka: \n', msg)

        #for key, value in msg.items(): # append su lista per problemi al JSON
        #    kafka_message.append(str(key))
        #    kafka_message.append(str(value))
    
    #print(json.dumps(kafkamessage))
    
        try:
            print("Sending message to Kafka is on going ...\n")
            p.produce(topic, key=timing, value=json.dumps(msg), callback=delivery_callback)
            print(p)
            print("Message sent!\n")
        except BufferError:
            print('%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(p))
            p.poll(0)
           
    #--------------------------------Creating JSON to pack kafka message-------------------------#
           

app = Flask(__name__)

@app.route('/')
def get_incomes_0():
    return ('localhost:5000/all -> print msg  localhost:5000/performance -> print performance')

@app.route('/all')
def get_incomes_1():
    return jsonify(kafka_message)

@app.route('/performance')
def get_incomes_2():
    return (performance_list)

if __name__ == '__main__':
    app.run(debug = False, host='0.0.0.0', port=5000)
   
#with open('output.txt', 'a') as f:
#    for item in minValues_list:
#        f.write('minValues_list\n')
#        f.write(str(item))
#        f.write('\n')
#        #print('minValues_list: \n', item)
#
#    for item in maxValues_list:
#        f.write('maxValues_list\n')
#        f.write(str(item))
#        f.write('\n')
#        #print('maxValues_list: \n', item)
#
#    for item in meanValues_list:
#        f.write('meanValues_list\n')
#        f.write(str(item))
#        f.write('\n')
#        #print('meanValues_list: \n', item)
#
#    for item in stdValues_list:
#        f.write('stdValues_list\n')
#        f.write(str(item))
#        f.write('\n')
#        #print('stdValues_list: \n', item)
#
#    for autocorrelation_i in autocorrelation_list:
#        f.write('autocorrelation_i\n')
#        f.write(str(autocorrelation_i))
#        f.write('\n')
#        #print('AUTOCORRELATION ----> ', autocorrelation_i)
#
#    f.write('STATIONARITY')
#    f.write(str(stationarity))
#    f.write('\n')
#    print('STATIONARITY ----> ', stationarity)
#
#    f.write('seasonability_i')
#    for seasonability_i in seasonability_list:
#        f.write(str(seasonability_i))
#        #f.write('\n')
#    
#    print('ciao oooooo ----> ')
#   
#    for item in Oneh_monitor:
#        f.write('Oneh_monitor\n')
#        print('ciao oooooo2 ----> ')
#   
#        f.write(str(item))
#        f.write('\n')
#        #print('1h Monitoring: \n', item)
#
#    for item in Twoh_monitor:
#        f.write('Twoh_monitor\n')
#        f.write(str(item))
#        f.write('\n')
#        #print('2h Monitoring: \n', item)
#
#    for item in Threeh_monitor:
#        f.write('Threeh_monitor\n')
#        f.write(str(item))
#        f.write('\n')
#        #print('3h Monitoring: \n', item) 

#print('Counter = ', counter)
########################################################################################################

#for metric in metrics_to_evaluate:
#    metric_data = prom.get_metric_range_data(
#        metric_name=metric,
#        label_config=label_config,
#        start_time=start_time,
#        end_time=end_time,
#        chunk_size=chunk_size,
#    )
#    metric_object_list = MetricsList(metric_data) #metric_object_list will be initialized as
#                                                  #a list of Metric objects for all the
#                                                  #metrics downloaded using get_metric query
#     #    my_metric_object = metric_object_list[0] # one of the metrics from the list
#    #print(my_metric_object)
#
#    fields = ['Date', 'Time', 'CPULoad'] 
#  
#    for item in metric_object_list:
#        print(item)
#
#    metric_object_list.to_csv('./export_dataframe.csv')
#
#----------------------------------------SECOND---------------------------------------------#
#---------calcoli il valore di max, min, avg, dev_std della metriche per 1h,3h, 12h---------#

#for item in metric_data:
#    #metric_df = MetricRangeDataFrame(metric_data)
#    metric_df = MetricRangeDataFrame(item)
#    print(metric_df.head())
#    param1= "cpuLoad"
#    param2= metric_df['value'].max()
#    param3= metric_df['value'].min()
#    param4= metric_df['value'].mean()
#    param4= metric_df['value'].std()


#########################################################################

#from confluent_kafka import Producer
#import sys
#
#if __name__ == '__main__':
#    if len(sys.argv) != 3:
#        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
#        sys.exit(1)
#
#    broker = sys.argv[1]
#    topic = sys.argv[2]
#
#    # Producer configuration
#    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
#    conf = {'bootstrap.servers': broker}
#
#    # Create Producer instance
#    p = Producer(**conf)
#
#    # Optional per-message delivery callback (triggered by poll() or flush())
#    # when a message has been successfully delivered or permanently
#    # failed delivery (after retries).
#    def delivery_callback(err, msg):
#        if err:
#            sys.stderr.write('%% Message failed delivery: %s\n' % err)
#        else:
#            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
#                             (msg.topic(), msg.partition(), msg.offset()))
#
#    # Read lines from stdin, produce each line to Kafka
#    for line in sys.stdin:
#        try:
#            # Produce line (without newline)
#            p.produce(topic, line.rstrip(), callback=delivery_callback)
#
#        except BufferError:
#            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
#                             len(p))
#
#        # Serve delivery callback queue.
#        # NOTE: Since produce() is an asynchronous API this poll() call
#        #       will most likely not serve the delivery callback for the
#        #       last produce()d message.
#        p.poll(0)
#
#    # Wait until all messages have been delivered
#    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
#    p.flush()

#########################################################################

#for item1 in metric_object_list:
#    with open('export_dataframe.csv', 'a') as f:
#            csv_writer = csv.writer(f)
#            csv_writer.writerow(fields)
#            csv_writer.writerow(item1)
#
#print("#-------------------#")




#nuovo_file = openpyxl.Workbook()
#sheet = nuovo_file.active
##sheet.title ='cpuload’
#nuovo_file.save('FirstQUERY.xlsx')
#sheet = nuovo_file.get_sheet_by_name('Sheet')
#multiple_cells = sheet['A1':'D60']
#
#
#
##nuovo_file.save('file_esempio.xlsx')
#
#for item in metric_object_list:
#    print(item)
#    for row in multiple_cells:
#        for cell in row:
#            cell = item
#
#
#
##    csv_writer.writerows(item)
#
##print(metric_object_list.index[])
#
#
#metric_df = MetricRangeDataFrame(metric_data)
##trasforma in una data frame di panda
##pip install prometheus-api-client
##python -m pip install statsmodels
#
##my_metric_object = {'product': ['computer', 'tablet', 'printer', 'laptop'],
##       'price': [850, 200, 150, 1300]
##        }
#
##df = pd.DataFrame(my_metric_object)
#
##df.to_csv(r'C:\Users\Benedetto Marchi\OneDrive\Desktop\export_dataframe.csv', index=False, header=True)
#
##print(df)
# 
#
#    
##rows = [ ['XYZ', '011', '2000'], 
# #        ['ABC', '012', '8000'],
# #        ['PQR', '351', '5000'],
# #        ['EFG', '146', '10000'] ] 
#  
#
#print("ok")
#
