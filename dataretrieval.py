from flask import Flask, jsonify
import mysql.connector
from mysql.connector import errorcode


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
    app = Flask(__name__)

    @app.route('/')
    def home():
        return ('DATARETRIVAL HOME')

    @app.route('/all') 
    def get_metrics():
        all = []

        # 1hMetrics        
        cursordb.execute("SELECT * FROM 1hMetrics;")
        all.append("#####===---=====---1H-Metrics---=====---#####")
        for item in cursordb : all.append(item)

        # 1hAutocorrelation
        cursordb.execute("SELECT * FROM 1hAutocorrelation;")
        all.append("#####===---=====---1H-Autocorrelation---=====---#####")
        for item in cursordb : all.append(item)

        # 1hStationarity
        cursordb.execute("SELECT * FROM 1hStationarity;")
        all.append("#####===---=====---1H-Stationarity---=====---#####")
        for item in cursordb : all.append(item)
        
        # 1hSeasonability
        cursordb.execute("SELECT * FROM 1hSeasonability;")
        all.append("#####===---=====---1H-Seasonability---=====---#####")
        for item in cursordb : all.append(item)

        # 1hPrediction
        cursordb.execute("SELECT * FROM 1hPrediction;")
        all.append("#####===---=====---3H-Prediction---=====---#####")
        for item in cursordb : all.append(item)

        # --====================================================-- #
        
        # 3hMetrics        
        cursordb.execute("SELECT * FROM 3hMetrics;")
        all.append("#####===---=====---3H-Metrics---=====---#####")
        for item in cursordb : all.append(item)

        # 3hAutocorrelation
        cursordb.execute("SELECT * FROM 3hAutocorrelation;")
        all.append("#####===---=====---3H-Autocorrelation---=====---#####")
        for item in cursordb : all.append(item)

        # 3hStationarity
        cursordb.execute("SELECT * FROM 3hStationarity;")
        all.append("#####===---=====---3H-Stationarity---=====---#####")
        for item in cursordb : all.append(item)
                
        # 3hSeasonability
        cursordb.execute("SELECT * FROM 3hSeasonability;")
        all.append("#####===---=====---3H-Seasonability---=====---#####")
        for item in cursordb : all.append(item)

        # 3hPrediction
        cursordb.execute("SELECT * FROM 3hPrediction;")
        all.append("#####===---=====---3H-Prediction---=====---#####")
        for item in cursordb : all.append(item)
        
        # --====================================================-- #
        
        # 12hMetrics        
        cursordb.execute("SELECT * FROM 12hMetrics;")
        all.append("#####===---=====---12H-Metrics---=====---#####")
        for item in cursordb : all.append(item)

        # 12hAutocorrelation
        cursordb.execute("SELECT * FROM 12hAutocorrelation;")
        all.append("#####===---=====---12H-Autocorrelation---=====---#####")
        for item in cursordb : all.append(item)

        # 12hStationarity
        cursordb.execute("SELECT * FROM 12hStationarity;")
        all.append("#####===---=====---12H-Stationarity---=====---#####")
        for item in cursordb : all.append(item)
                
        # 12hSeasonability
        cursordb.execute("SELECT * FROM 12hSeasonability;")
        all.append("#####===---=====---12H-Seasonability---=====---#####")
        for item in cursordb : all.append(item)

        # 12hPrediction
        cursordb.execute("SELECT * FROM 12hPrediction;")
        all.append("#####===---=====---12H-Prediction---=====---#####")
        for item in cursordb : all.append(item)
         
        return jsonify(all)

    @app.route('/metrics/') 
    def all_metric():
        a=[]
        sql = "SELECT * FROM 1hMetrics;"
        cursordb.execute(sql)
        a.append("#####===---=====---1H-Metrics---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 3hMetrics;"
        cursordb.execute(sql)
        a.append("#####===---=====---3H-Metrics---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 12hMetrics;"
        cursordb.execute(sql)
        a.append("#####===---=====---12H-Metrics---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)
        
        return jsonify(a)

    @app.route('/metrics/<name>') 
    def show_metric_by_name(name):
        a = []

        sql = "SELECT * FROM 1hMetrics WHERE metric = '{0}';".format(name)
        cursordb.execute(sql)
        a.append("#####===---=====---1H-Metrics---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 3hMetrics WHERE metric = '{0}';".format(name)
        cursordb.execute(sql)
        a.append("#####===---=====---3H-Metrics---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 12hMetrics WHERE metric = '{0}';".format(name)
        cursordb.execute(sql)
        a.append("#####===---=====---12H-Metrics---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        return jsonify(a)

    @app.route('/autocorrelation/') 
    def all_autocorrelation():
        a=[]
        sql = "SELECT * FROM 1hAutocorrelation;"
        cursordb.execute(sql)
        a.append("#####===---=====---1H-Autocorrelation---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 3hAutocorrelation;"
        cursordb.execute(sql)
        a.append("#####===---=====---3H-Autocorrelation---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 12hAutocorrelation;"
        cursordb.execute(sql)
        a.append("#####===---=====---12H-Autocorrelation---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)
        
        return jsonify(a)

    @app.route('/autocorrelation/<name>') 
    def show_autocorrelation_by_name(name):
        a = []

        sql = "SELECT * FROM 1hAutocorrelation WHERE metric = '{0}';".format(name)
        cursordb.execute(sql)
        a.append("#####===---=====---1H-Autocorrelation---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 3hAutocorrelation WHERE metric = '{0}';".format(name)
        cursordb.execute(sql)
        a.append("#####===---=====---3H-Autocorrelation---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 12hAutocorrelation WHERE metric = '{0}';".format(name)
        cursordb.execute(sql)
        a.append("#####===---=====---12H-Autocorrelation---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        return jsonify(a)
    
    @app.route('/stationarity/') 
    def all_stationarity():
        a=[]
        sql = "SELECT * FROM 1hStationarity;"
        cursordb.execute(sql)
        a.append("#####===---=====---1H-Stationarity---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 3hStationarity;"
        cursordb.execute(sql)
        a.append("#####===---=====---3H-Stationarity---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 12hStationarity;"
        cursordb.execute(sql)
        a.append("#####===---=====---12H-Stationarity---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)
        
        return jsonify(a)

    @app.route('/stationarity/<name>') 
    def show_stationarity_by_name(name):
        a = []

        sql = "SELECT * FROM 1hStationarity WHERE metric = '{0}';".format(name)
        cursordb.execute(sql)
        a.append("#####===---=====---1H-Stationarity---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 3hStationarity WHERE metric = '{0}';".format(name)
        cursordb.execute(sql)
        a.append("#####===---=====---3H-Stationarity---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 12hStationarity WHERE metric = '{0}';".format(name)
        cursordb.execute(sql)
        a.append("#####===---=====---12H-Stationarity---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        return jsonify(a)

    @app.route('/seasonability/') 
    def all_seasonability():
        a=[]
        sql = "SELECT * FROM 1hSeasonability;"
        cursordb.execute(sql)
        a.append("#####===---=====---1H-Seasonability---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 3hSeasonability;"
        cursordb.execute(sql)
        a.append("#####===---=====---3H-Seasonability---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 12hSeasonability;"
        cursordb.execute(sql)
        a.append("#####===---=====---12H-Seasonability---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)
        
        return jsonify(a)

    @app.route('/seasonability/<name>') 
    def show_seasonability_by_name(name):
        a = []

        sql = "SELECT * FROM 1hSeasonability WHERE metric = '{0}';".format(name)
        cursordb.execute(sql)
        a.append("#####===---=====---1H-Seasonability---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 3hSeasonability WHERE metric = '{0}';".format(name)
        cursordb.execute(sql)
        a.append("#####===---=====---3H-Seasonability---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 12hSeasonability WHERE metric = '{0}';".format(name)
        cursordb.execute(sql)
        a.append("#####===---=====---12H-Seasonability---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        return jsonify(a)

    @app.route('/prediction/') 
    def all_prediction():
        a=[]
        sql = "SELECT * FROM 1hPrediction;"
        cursordb.execute(sql)
        a.append("#####===---=====---1H-Prediction---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 3hPrediction;"
        cursordb.execute(sql)
        a.append("#####===---=====---3H-Prediction---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 12hPrediction;"
        cursordb.execute(sql)
        a.append("#####===---=====---12H-Prediction---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)
        
        return jsonify(a)

    @app.route('/prediction/<name>') 
    def show_prediction_by_name(name):
        a = []

        sql = "SELECT * FROM 1hPrediction WHERE metric = '{0}';".format(name)
        cursordb.execute(sql)
        a.append("#####===---=====---1H-Prediction---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 3hPrediction WHERE metric = '{0}';".format(name)
        cursordb.execute(sql)
        a.append("#####===---=====---3H-Prediction---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        sql = "SELECT * FROM 12hPrediction WHERE metric = '{0}';".format(name)
        cursordb.execute(sql)
        a.append("#####===---=====---12H-Prediction---=====---#####")
        a.append("METRIC,MAX,MIN,MEAN,DEV_STD")
        for item in cursordb : a.append(item)

        return jsonify(a)

    if __name__ == '__main__':
        app.run(debug = False, host='0.0.0.0', port=5050)

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("ACCESS_DENIED_ERROR")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("BAD_DB")
    else:
        print(err)
        
db.close()
