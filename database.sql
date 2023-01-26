-- """INSERT INTO 1hMetrics (metric,max,min,mean,std) VALUES (%s,%s,%s,%s,%s);"""
-- """INSERT INTO 1hAutocorrelation (metric,value) VALUES (%s,%s);"""
-- """INSERT INTO 1hStationarity (metric,adf,pvalue,usedlag,nobs,criticalvalues,icbest) VALUES (%s,%s,%s,%s,%s,%s,%s);"""
-- """INSERT INTO 1hPrediction (metric,max,min,mean) VALUES (%s,%s,%s);"""
-- """INSERT INTO 1hSeasonability (metric,value) VALUES (%s,%s);"""
CREATE TABLE 1hMetrics (ID INT AUTO_INCREMENT, metric varchar(255), max DOUBLE,min DOUBLE,mean DOUBLE,std DOUBLE,PRIMARY KEY(ID));
CREATE TABLE 1hAutocorrelation (ID INT AUTO_INCREMENT, metric varchar(255),value DOUBLE,PRIMARY KEY(ID));
CREATE TABLE 1hStationarity (ID INT AUTO_INCREMENT, metric varchar(255),adf DOUBLE,pvalue DOUBLE,usedlag DOUBLE,nobs DOUBLE,criticalvalues varchar(255),icbest DOUBLE,PRIMARY KEY(ID));
CREATE TABLE 1hSeasonability (ID INT AUTO_INCREMENT, metric varchar(255),value DOUBLE,PRIMARY KEY(ID));
CREATE TABLE 1hPrediction (ID INT AUTO_INCREMENT, metric varchar(255), max DOUBLE,min DOUBLE,mean DOUBLE,PRIMARY KEY(ID));
CREATE TABLE 3hMetrics (ID INT AUTO_INCREMENT, metric varchar(255), max DOUBLE,min DOUBLE,mean DOUBLE,std DOUBLE,PRIMARY KEY(ID));
CREATE TABLE 3hAutocorrelation (ID INT AUTO_INCREMENT, metric varchar(255),value DOUBLE,PRIMARY KEY(ID));
CREATE TABLE 3hStationarity (ID INT AUTO_INCREMENT, metric varchar(255),adf DOUBLE,pvalue DOUBLE,usedlag DOUBLE,nobs DOUBLE,criticalvalues varchar(255),icbest DOUBLE,PRIMARY KEY(ID));
CREATE TABLE 3hSeasonability (ID INT AUTO_INCREMENT, metric varchar(255),value DOUBLE,PRIMARY KEY(ID));
CREATE TABLE 3hPrediction (ID INT AUTO_INCREMENT, metric varchar(255), max DOUBLE,min DOUBLE,mean DOUBLE,PRIMARY KEY(ID));
CREATE TABLE 12hMetrics (ID INT AUTO_INCREMENT, metric varchar(255), max DOUBLE,min DOUBLE,mean DOUBLE,std DOUBLE,PRIMARY KEY(ID));
CREATE TABLE 12hAutocorrelation (ID INT AUTO_INCREMENT, metric varchar(255),value DOUBLE,PRIMARY KEY(ID));
CREATE TABLE 12hStationarity (ID INT AUTO_INCREMENT, metric varchar(255),adf DOUBLE,pvalue DOUBLE,usedlag DOUBLE,nobs DOUBLE,criticalvalues varchar(255),icbest DOUBLE,PRIMARY KEY(ID));
CREATE TABLE 12hSeasonability (ID INT AUTO_INCREMENT, metric varchar(255),value DOUBLE,PRIMARY KEY(ID));
CREATE TABLE 12hPrediction (ID INT AUTO_INCREMENT, metric varchar(255), max DOUBLE,min DOUBLE,mean DOUBLE,PRIMARY KEY(ID));
