import csv
#import openpyxl
import pandas as pd
from prometheus_api_client import PrometheusConnect, MetricsList, MetricSnapshotDataFrame, MetricRangeDataFrame
from datetime import timedelta
from prometheus_api_client.utils import parse_datetime


#Connection to Prometheus
prom = PrometheusConnect(url="http://15.160.61.227:29090", disable_ssl=True)

#Query on Prom

start_time = parse_datetime("1h")
end_time = parse_datetime("now")
chunk_size = timedelta(minutes=5)
label_config = {'job': 'summary'} 
#solo il job,cosi mi fa la media di tutte le istanze dei job
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


for item in metric_object_list:
    with open('export_dataframe.csv', 'a') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(fields)
            csv_writer.writerow(item)

print("#-------------------#")




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
