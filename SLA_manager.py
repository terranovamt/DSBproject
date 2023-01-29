from prometheus_api_client import PrometheusConnect, MetricRangeDataFrame
from prometheus_api_client.utils import parse_datetime
from flask import Flask, jsonify, request
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from datetime import timedelta
import json
import requests

SLAset_list = []
range_list = []
violation_list = []
futureViolation_list = []

def getViolation():
    time_to_evaluate = ['12h','3h','1h']
    label_config = {'nodeName': 'sv122','job': 'summary'} 
    chunk_size = timedelta(minutes=10)
    end_time = parse_datetime("now") 
    prom = PrometheusConnect(url="http://15.160.61.227:29090", disable_ssl=True)

    violation_list.clear() 
    futureViolation_list.clear()
    counter = 0
    for metric in SLAset_list:        
        for timing in time_to_evaluate:
            
            start_time = parse_datetime(timing)
            try:
                metric_data = prom.get_metric_range_data(
                    metric_name=metric,
                    label_config=label_config,
                    start_time=start_time,
                    end_time=end_time,
                    chunk_size=chunk_size,
                )
                metric_df = MetricRangeDataFrame(metric_data) 
                
            except:
                continue
            for i in range(len(metric_df)):
                if  metric_df['value'][i] < range_list[counter][0] or  metric_df['value'][i] > range_list[counter][1]:
                    
                    violation = {
                        "MetricName": metric_df['__name__'][i],
                        "Value": metric_df['value'][i],
                        "Timestamp": metric_df['value'].keys()[i],                
                        "Duration": timing
                    }
                    violation_list.append(violation)


            metric_resampling = metric_df['value'].resample(rule='1T').mean()
            model_pred = ExponentialSmoothing(metric_resampling, trend='add', seasonal='add',seasonal_periods=10).fit() 
            prediction = model_pred.forecast(steps=10)
            prediction_list = list(prediction)
            for i in range(len(prediction_list)):
                if prediction_list[i] < range_list[counter][0] or prediction_list[i] > range_list[counter][1]:
                    #print("Violations occur in 10minutes: ", prediction.keys()[i], "VALORE: ", prediction_list[i])
                    
                    futureviolation = {
                        "MetricName": metric_df['__name__'][i],
                        "Value": prediction_list[i],
                        "Timestamp": prediction.keys()[i],
                        
                    }
                    futureViolation_list.append(futureviolation)
        counter +=1
        print(counter)
   

def post_to_ETL(data): 
    url = 'http://etl_data_pipeline:5000/SLAset'
    SLAset = list(data.keys())
    range = list(data.values())

    #-----------------Cleaning if list is full--------------------#
    if len(SLAset_list) > 0: 
        SLAset_list.clear() 

    for i in SLAset:
        SLAset_list.append(i)

    #-----------------JSON creation for posting------------#
    metricPOST = { 
              "0": SLAset_list[0], 
              "1": SLAset_list[1],
              "2": SLAset_list[2],
              "3": SLAset_list[3],
              "4": SLAset_list[4]
              }
    request = requests.post(url, json= metricPOST)

    #-----------------Cleaning if list is full--------------------#
    if len(range_list) > 0:
        range_list.clear() 
    #-----------------Cleaning if list is full--------------------#
    for i in range:
            range_list.append(i)

    print(request.text)


app = Flask(__name__)

#-----------------Post to modify SLA set & range-------------------#
@app.route('/SLA_Manager', methods = ['POST'])
def modifySLA(): 
    jsonREQ = request.json
    data = json.loads(request.data)
    post_to_ETL(data)
    getViolation()
    return jsonREQ

#----------------------Get to show violation-----------------------#
@app.route('/show_Violation') 
def show_Violation():
    return jsonify(violation_list)

#---------------Get to show violation by timing--------------------#
@app.route('/Violations_Number') 
def Violations_Number():
    Oneh =0
    Threeh = 0
    Twelveh = 0
    for item in violation_list:
        if item['Duration'] == '1h': Oneh += 1
        if item['Duration'] == '3h': Threeh += 1
        if item['Duration'] == '12h': Twelveh += 1

    violationNum = {
        "Number of violations in 1h:": Oneh,
        "Number of violations in 3h:": Threeh,
        "Number of violations in 12h:": Twelveh
    }
    return jsonify(violationNum)

#-------------Get to show violation by timing and name-----------#
@app.route('/SLA_status') 
def SLA_status():
    status = []
    for metric_name in SLAset_list:
        Oneh =0
        Threeh = 0
        Twelveh = 0
        for item in violation_list:
            if item['MetricName'] == metric_name and item['Duration'] == '1h': Oneh += 1
            if item['MetricName'] == metric_name and item['Duration'] == '3h': Threeh += 1
            if item['MetricName'] == metric_name and item['Duration'] == '12h': Twelveh += 1

        violationStatus = {
            "Metric Name:": metric_name,
            "Number of violations in 1h:": Oneh,
            "Number of violations in 3h:": Threeh,
            "Number of violations in 12h::": Twelveh
        }
        status.append(violationStatus)
    return jsonify(status)

#----------------Get to show predict violations---------------#
@app.route('/predict_Violations') 
def predict_Violations():
    return jsonify(futureViolation_list)

#------------Get to show predict violations by name-----------#
@app.route('/predict_Violations&Name') 
def predict_ViolationsName():
    status = []
    for metric_name in SLAset_list:
        violNum = 0
        for item in futureViolation_list:
            if item['MetricName'] == metric_name:
                violNum += 1
        violationST = {
            "MetricName:": metric_name,
            "Number of possible violation in 10 minutes": violNum,
        }
        status.append(violationST)
    return jsonify(status)

if __name__ == '__main__':
    app.run(debug = False, host='0.0.0.0', port=5100)

 