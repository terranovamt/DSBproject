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
import sys
import pandas as pd

broker = "kafka:9092"
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


print('#-----------------Connecting-----------------#\n')
print('                  ..........                  \n')
print('                  ..........                  \n')
print('                  ..........                  \n')
prom = PrometheusConnect(url="http://15.160.61.227:29090", disable_ssl=True)
print('#------------Connection succed !-------------#\n')

#----------------------------------------PROMETHEUS' QUERY---------------------------------------------#
time_to_evaluate = ['12h','3h','1h']
metrics_to_evaluate = ['cpuLoad', 'cpuTemp', 'diskUsage', 'availableMem', 'networkThroughput'] #
end_time = parse_datetime("now")
chunk_size = timedelta(minutes=10)
label_config = {'nodeName': 'sv122','job': 'summary'} 
counter = 0
performance_list = [] 


for metric in metrics_to_evaluate: 
    print("\n",metric)      
    for timing in time_to_evaluate:    
        minValues_list = []
        maxValues_list = []
        meanValues_list = []
        stdValues_list = []
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
        
        
        #--------------------------Evaluation of MAX, min, mean and std_dev----------------------------#
        #print("Evaluation of MAX, min, mean and std_dev is on going")
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

        #-----------------Evaluation of AutoCorrelation, Stationarity & Seasonability-------------------#
        t0_evaluation = time.time()
       #print("Evaluation of AutoCorrelation is on going ...")
        autocorrelation = acf(metric_df['value']) 
        autocorrelation_list = autocorrelation.tolist()
        del autocorrelation_list[0]
        # for autocorrelation_i in autocorrelation_list:
        #    print('AUTOCORRELATION ----> ', autocorrelation_i)

       #print("Evaluation of Stationariety is on going ...   ")
        stationarity = adfuller(metric_df['value'],autolag='AIC') 
       #print('STATIONARITY ----> ', stationarity)
       #print("Evaluation of Seasonability is on going ...   ")
        seasonability = seasonal_decompose(metric_df['value'], model='additive', period=10)
        seasonability_list = seasonability.seasonal.tolist() 
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
        #print("PREDICTION is on going ...                     ")
        model_MAX = ExponentialSmoothing(MAX_resampling, trend='add', seasonal='add',seasonal_periods=5).fit()
        model_min = ExponentialSmoothing(min_resampling, trend='add', seasonal='add',seasonal_periods=5).fit()
        model_mean = ExponentialSmoothing(mean_resampling, trend='add', seasonal='add',seasonal_periods=5).fit() 
        
        prediction_MAX =[]
        t = model_MAX.forecast(5).to_dict()    
        for key, value in t.items():
            prediction_MAX.append(str(value))  
        #print("Prediction of MAX ---> \n", prediction_max)
        prediction_min =[]
        t = model_min.forecast(5).to_dict() 
        for key, value in t.items():
            prediction_min.append(str(value))  
        #print("Prediction of min ---> \n", prediction_min)  
        prediction_mean =[]
        t = model_mean.forecast(5).to_dict() 
        for key, value in t.items():
            prediction_mean.append(str(value))  
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
                
        

    #--------------------------------Creating JSON to pack kafka message-------------------------#
       

        msg = {
                "Metric": metric,
                "MAX_value":max_value,
                "min_value": min_value,
                "mean_value": mean_value,
                "std_value": std_value,                
                "Autocorrelation": json.dumps(autocorrelation_list),
                "Stationarity": json.dumps(stationarity),
                "Seasonability": json.dumps(seasonability_list),
                "Prediction_MAX": prediction_MAX,
                "Prediction_min": prediction_min,
                "Prediction_Mean": prediction_mean
                }                

        counter += 1
        try:
            #print("Sending message to Kafka is on going ...")
            p.produce(topic, key=timing, value=json.dumps(msg), callback=delivery_callback)
            print("COUTER -->",counter,"",timing,"",p)
            #print("Message sent!")
        except BufferError:
            print('%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(p))
            p.poll(0)

#--------------------------------Creating JSON to pack kafka message-------------------------#
         
app = Flask(__name__)

@app.route('/')
def get_incomes_0():
    return ('/all -> print msg  /performance -> print performance')

@app.route('/all')
def get_incomes_1():
    return jsonify(kafka_message)

@app.route('/performance')
def get_incomes_2():
    return (performance_list)

@app.route('/SLAset', methods = ['POST']) 
def SLAset():
    req = request.json 
    j = 0
    for i in range(len(req)):
        metrics_to_evaluate[j] = req[str(i)]
        j += 1
   
    return 1

if __name__ == '__main__':
    app.run(debug = False, host='0.0.0.0', port=5000)
