import csv
#import openpyxl
import pandas as pd
from prometheus_api_client import PrometheusConnect, MetricsList, MetricSnapshotDataFrame, MetricRangeDataFrame
from datetime import timedelta
from prometheus_api_client.utils import parse_datetime


#Connection to Prometheus
prom = PrometheusConnect(url="http://15.160.61.227:29090", disable_ssl=True)

#Query on Prom
#calcoli un set di metadati con i relativi valori (autocorrelazione? stazionarietà? stagionalità?)

start_time = parse_datetime("1h")
end_time = parse_datetime("now")
chunk_size = timedelta(minutes=5)
label_config = {'job': 'summary'} 
metrics_to_evaluate = ['cpuLoad', 'availableMem', 'diskUsage']
for metric in metrics_to_evaluate:
    metric_data = prom.get_metric_range_data(
        metric_name=metric,
        label_config=label_config,
        start_time=start_time,
        end_time=end_time,
        chunk_size=chunk_size,
    )
    metric_object_list = MetricsList(metric_data) #metric_object_list will be initialized as
                                                  #a list of Metric objects for all the
                                                  #metrics downloaded using get_metric query
     #    my_metric_object = metric_object_list[0] # one of the metrics from the list
    #print(my_metric_object)

    fields = ['Date', 'Time', 'CPULoad'] 
  
    for item in metric_object_list:
        print(item)

    metric_object_list.to_csv('./export_dataframe.csv')


#- calcoli il valore di max, min, avg, dev_std della metriche per 1h,3h, 12h;

metric_df = MetricRangeDataFrame(metric_data)
print(metric_df.head())
param1= "cpuLoad"
param2= metric_df['value'].max()
param3= metric_df['value'].min()
param4= metric_df['value'].mean()
param4= metric_df['value'].std()


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
