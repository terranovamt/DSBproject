from prometheus_api_client import PrometheusConnect, MetricsList, MetricSnapshotDataFrame, MetricRangeDataFrame
from prometheus_api_client.utils import parse_datetime
#from confluent_kafka import Producer
#from flask import Flask, jsonify, request
from datetime import timedelta
from statsmodels.tsa.stattools import acf, adfuller, kpss
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.holtwinters import ExponentialSmoothing
import json
import time
import csv
#import openpyxl
import pandas as pd


def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s partition [%d] @ %d\n' %
                         (msg.topic(), msg.partition(), msg.offset()))

configuration = {'bootstrap.servers' : 'broker_kafka:9092'}
topic = "prometheusdata"

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
time_to_evaluate = ['1h', '3h', '12h']
metrics_to_evaluate = ['cpuLoad', 'cpuTemp', 'diskUsage', 'availableMem', 'networkThroughput']
end_time = parse_datetime("now")
chunk_size = timedelta(minutes=10)
label_config = {'job': 'summary'} 
counter = 0
for time in time_to_evaluate:       
    start_time = parse_datetime(time)
    for metric in metrics_to_evaluate:
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
        minValues_list = []
        maxValues_list = []
        meanValues_list = []
        stdValues_list = []
        Oneh_monitor = []
        Twoh_monitor = []
        Threeh_monitor = []
        #--------------------------Evaluation of MAX, min, mean and std_dev----------------------------#
        print("Evaluation of MAX, min, mean and std_dev is on going \n")
        print('                       ..........                    \n')
        print('                       ..........                    \n')
        max_value = round(metric_df['value'].max(), 2)
        min_value = round(metric_df['value'].min(), 2)
        mean_value = round(metric_df['value'].mean(), 2)
        std_value = round(metric_df['value'].std(), 2)
        print("\n               Monitoring done!            \n")
        print("---------------->MAX value: ", max_value)
        print("---------------->min value: ", min_value)
        print("---------------->MEAN value: ", mean_value)
        print("---------------->STD_dev value: ", std_value)
        print('                                           \n\n')
        minValues_list.append(min_value) 
        maxValues_list.append(max_value) 
        meanValues_list.append(mean_value)
        stdValues_list.append(std_value) 
        
        #-----------------Evaluation of AutoCorrelation, Stationarity & Seasonability-------------------#
        print("Evaluation of AutoCorrelation is on going ...\n")
        autocorrelation = acf(metric_df['value']) 
        autocorrelation_list = autocorrelation.tolist()
        del autocorrelation_list[0]
        for autocorrelation_i in autocorrelation_list:
            print('AUTOCORRELATION ----> ', autocorrelation_i)

        print("Evaluation of Stationariety is on going ...   \n")
        stationarity = adfuller(metric_df['value'],autolag='AIC') 
        print('STATIONARITY ----> ', stationarity)

        print("Evaluation of Seasonability is on going ...   \n")
        seasonability = seasonal_decompose(metric_df['value'], model='additive', period=10)
        seasonability_list = seasonability.seasonal.tolist() 
        for seasonability_i in seasonability_list:
            print('SEASONABILITY ----> ', seasonability_i)

        #-------------------------------Prediction of each metric--------------------------------------#
        #---------->RESAMPLING
        print("Prediction process is started now            \n")
        print('                     ..........              \n')
        print('                     ..........              \n')
        print("RESAMPLING is on going ...                   \n")
        MAX_resampling = metric_df['value'].resample(rule='2T').max()
        print(" MAX RESAMPLING ---> \n", MAX_resampling)
        min_resampling = metric_df['value'].resample(rule='2T').min()
        print(" MIN RESAMPLING ---> \n", min_resampling)
        mean_resampling = metric_df['value'].resample(rule='2T').mean()   
        print(" MEAN RESAMPLING ---> \n", mean_resampling) 
        
        #---------->PREDICTION
        print('                     ..........              \n')
        print('                     ..........              \n')
        print("PREDICTION is on going ...                   \n")
        tsmodel_MAX = ExponentialSmoothing(MAX_resampling, trend='add', seasonal='add',seasonal_periods=5).fit()
        tsmodel_min = ExponentialSmoothing(min_resampling, trend='add', seasonal='add',seasonal_periods=5).fit()
        tsmodel_mean = ExponentialSmoothing(mean_resampling, trend='add', seasonal='add',seasonal_periods=5).fit() 
        
        prediction_max = tsmodel_MAX.forecast(5) 
        print("Prediction of MAX ---> \n", prediction_max)
        prediction_min = tsmodel_min.forecast(5)  
        print("Prediction of min ---> \n", prediction_min)  
        prediction_mean = tsmodel_mean.forecast(5)        
        print("Prediction of mean ---> \n", prediction_mean) 

        monitoring_values = {
            "Metrics":{
                "Metric Name": metric,
                "Timing": time
            } 
        }
        if (time == '1h'):
            Oneh_monitor.append(monitoring_values)
        if (time == '2h'):
            Twoh_monitor.append(monitoring_values)
        if (time == '3h'):
            Threeh_monitor.append(monitoring_values)

        counter += 1

with open('output.txt', 'a') as f:
    for item in minValues_list:
        f.write('minValues_list')
        f.write(item)
        f.write('\n')
        #print('minValues_list: \n', item)

    for item in maxValues_list:
        f.write('maxValues_list')
        f.write(item)
        f.write('\n')
        #print('maxValues_list: \n', item)

    for item in meanValues_list:
        f.write('meanValues_list')
        f.write(item)
        f.write('\n')
        #print('meanValues_list: \n', item)

    for item in stdValues_list:
        f.write('stdValues_list')
        f.write(item)
        f.write('\n')
        #print('stdValues_list: \n', item)

    for autocorrelation_i in autocorrelation_list:
        f.write('stdValues_list')
        f.write(autocorrelation_i)
        f.write('\n')
        #print('AUTOCORRELATION ----> ', autocorrelation_i)

    f.write('STATIONARITY')
    f.write(stationarity)
    print('STATIONARITY ----> ', stationarity)

    for seasonability_i in seasonability_list:
        f.write('seasonability_i')
        f.write(seasonability_i)
        f.write('\n')
        #print('SEASONABILITY ----> ', seasonability_i)

    for item in Oneh_monitor:
        f.write('Oneh_monitor')
        f.write(item)
        f.write('\n')
        #print('1h Monitoring: \n', item)

    for item in Twoh_monitor:
        f.write('Twoh_monitor')
        f.write(item)
        f.write('\n')
        #print('2h Monitoring: \n', item)

    for item in Threeh_monitor:
        f.write('Threeh_monitor')
        f.write(item)
        f.write('\n')
        #print('3h Monitoring: \n', item) 

print('Counter = ', counter)
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
