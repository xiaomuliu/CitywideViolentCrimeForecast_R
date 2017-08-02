setwd("/Users/xiaomuliu/CrimeProject/ViolentCrimeForecastTool_R/")

# Set JAVA_HOME, set max. memory, and load rJava library
Sys.setenv(JAVA_HOME='/usr/libexec/java_home -v 1.7')
#options(java.parameters="-Xmx2g")
library(rJava)

# # Output Java version
# .jinit()
# print(.jcall("java/lang/System", "S", "getProperty", "java.version"))

# Load RJDBC library
library(DBI)
library(RJDBC)

# Create connection driver and open connection
jdbcDriver <- JDBC(driverClass="oracle.jdbc.OracleDriver", classPath="/Users/xiaomuliu/Oracle/instantclient_11_2/ojdbc6.jar")
conn <- dbConnect(jdbcDriver, "jdbc:oracle:thin:@//167.165.243.151:1521/dwhracdb.dwhrac.chicagopolice.local", "IIT_USER", "TSR1512")

sqlString <- "SELECT DATE1, YEAR, MONTH, DAY, DAYOFWEEK,
CASE WHEN        (MONTH = 12  AND DAY = 31 AND DAYOFWEEK = 5) OR
(MONTH = 01  AND DAY = 02 AND DAYOFWEEK = 1) OR
(MONTH = 01  AND DAY = 01) 
THEN 1
WHEN        MONTH = 01 AND DAY BETWEEN 15 AND 21 AND DAYOFWEEK = 1 
THEN 2
WHEN        MONTH = 02 AND DAY BETWEEN 15 AND 21 AND DAYOFWEEK = 1
THEN 3            
WHEN        MONTH = 05 AND DAY BETWEEN 25 AND 31 AND DAYOFWEEK = 1 
THEN 4
WHEN        (MONTH = 07  AND DAY = 03 AND DAYOFWEEK = 5) OR
(MONTH = 07  AND DAY = 05 AND DAYOFWEEK = 1) OR
(MONTH = 07  AND DAY = 04)  
THEN 5
WHEN        MONTH = 09 AND DAY BETWEEN 1 AND 7 AND DAYOFWEEK = 1 
THEN 6
WHEN        MONTH = 10 AND DAY BETWEEN 8 AND 14 AND DAYOFWEEK = 2  
THEN 7 
WHEN        MONTH = 10  AND DAY = 31 
THEN 8
WHEN        (MONTH = 11  AND DAY = 10 AND DAYOFWEEK = 5) OR
(MONTH = 11  AND DAY = 12 AND DAYOFWEEK = 1) OR
(MONTH = 11  AND DAY = 11)  
THEN 9
WHEN        MONTH = 11 AND DAY BETWEEN 22 AND 28 AND DAYOFWEEK = 4 
THEN 10    
WHEN        (MONTH = 12  AND DAY = 24 AND DAYOFWEEK = 5) OR
(MONTH = 12  AND DAY = 26 AND DAYOFWEEK = 1) OR
(MONTH = 12  AND DAY = 25) 
THEN 11
ELSE 0 
END AS HOLIDAY,
DAILY_TOTAL
FROM (
SELECT TRUNC(DATEOCC) AS DATE1,
YEAR,
MONTH,
DAY,
TO_CHAR(TO_DATE(YEAR||'/'||MONTH||'/'||DAY,'YYYY/MM/DD'),'D')  AS DAYOFWEEK,
COUNT(CURR_IUCR) AS DAILY_TOTAL
FROM CHRIS_DWH.CRIMES_ALLV
WHERE (DATEOCC BETWEEN TO_DATE(TO_CHAR(SYSDATE-820, 'MM-DD-YYYY') || ' 00:00:00', 'MM-DD-YYYY hh24:mi:ss') AND TO_DATE(TO_CHAR(SYSDATE-1, 'MM-DD-YYYY') || ' 23:59:59', 'MM-DD-YYYY hh24:mi:ss')) -- 2*365 + 90-day buffer
AND (CITY='CHICAGO') 
AND (CURR_IUCR IN ('0110','0130','041A','041B','0420','0430','0440','0479','051A','051B','0520','0530','0545','0560','1753','1754') 
OR CURR_IUCR LIKE '02__'
OR CURR_IUCR LIKE '03__'
OR CURR_IUCR LIKE '045_'
OR CURR_IUCR LIKE '046_'
OR CURR_IUCR LIKE '048_'
OR CURR_IUCR LIKE '049_'
OR CURR_IUCR LIKE '055_')
GROUP BY TRUNC(DATEOCC), YEAR, MONTH, DAY, TO_CHAR(TO_DATE(YEAR||'/'||MONTH||'/'||DAY,'YYYY/MM/DD'),'D')
ORDER BY TRUNC(DATEOCC) ASC
)"

TwoYearData <- dbGetQuery(conn,sqlString)

# # Close connection
# dbDisconnect(conn) 

load("ViolentCrimeData_portal.RData")

TwoYearData$DATEOCC <- as.Date(TwoYearData$DATE1,"%Y-%m-%d")
TwoYearData$DATE1 <- NULL
names(TwoYearData)[names(TwoYearData)=="DAYOFWEEK"] <- "DOW"
names(TwoYearData)[names(TwoYearData)=="DAILY_TOTAL"] <- "INC_CNT"

CrimeData.day <- aggregate(INC_CNT~DATEOCC+YEAR+MONTH+DAY+DOW+HOLIDAY,data=CrimeData, FUN=sum, na.rm=TRUE)
CrimeData.day <- CrimeData.day[order(CrimeData.day$DATEOCC),]
CrimeData.day$DOW <- factor(CrimeData.day$DOW, levels=c("Sun","Mon","Tue","Wed","Thu","Fri","Sat"))

TwoYearData$YEAR <- as.integer(TwoYearData$YEAR)
TwoYearData$MONTH <- as.integer(TwoYearData$MONTH)
TwoYearData$DAY <- as.integer(TwoYearData$DAY)
TwoYearData$DOW <- as.factor(as.integer(TwoYearData$DOW))
levels(TwoYearData$DOW) <- c("Sun","Mon","Tue","Wed","Thu","Fri","Sat")
TwoYearData$HOLIDAY <- as.factor(TwoYearData$HOLIDAY)
TwoYearData$INC_CNT <- as.integer(TwoYearData$INC_CNT)

#update the crime data
intersect.x <- match(TwoYearData$DATEOCC,CrimeData.day$DATEOCC,nomatch=0)
intersect.y <- match(CrimeData.day$DATEOCC,TwoYearData$DATEOCC,nomatch=0)
CrimeData.day$INC_CNT[intersect.x] <- TwoYearData$INC_CNT[intersect.y]
CrimeData.day <- merge(CrimeData.day,TwoYearData,all=TRUE)
CrimeData.day <- CrimeData.day[!duplicated(CrimeData.day$DATEOCC),]


# Detrending
library(MASS)
source("TimeSeriesFunction.R")

#Smooth out holiday cases
CrimeData.day$INC_CNT_s <- SmoothHoliday(CrimeData.day)

trendLen <- 730
CrimeData.buffer <- CrimeData.day[1:trendLen,]
CrimeData.nonbuffer <- CrimeData.day[(trendLen+1):nrow(CrimeData.day),]
CrimeData.nonbuffer$TStrend <- rep(NA,nrow(CrimeData.nonbuffer))
CrimeData.nonbuffer$TSdetrendRes <- rep(NA,nrow(CrimeData.nonbuffer))

Trend <- PredictTrend(CrimeData.day,trendLen,nlfit="IRLS") 
CrimeData.nonbuffer$TStrend <- Trend
CrimeData.nonbuffer$TSdetrendRes <- CrimeData.nonbuffer$INC_CNT_s-CrimeData.nonbuffer$TStrend  

## Load weather data
source("WeatherDataFunctions.R")
WeatherFilePath <- "/Users/xiaomuliu/CrimeProject/SpatioTemporalModeling/ExploratoryAnalysis/WeatherData/"
startDate <- "01/01/2001"
endDate <- "12/31/2014"
filename.daily <- paste(WeatherFilePath,'WeatherData_Daily_',as.character(as.Date(startDate, "%m/%d/%Y")),
                        '_',as.character(as.Date(endDate, "%m/%d/%Y")),'.csv',sep='')
WeatherData.daily <- read.csv(filename.daily)
WeatherData.daily$Date <- as.Date(WeatherData.daily$Date)
WeatherData.daily_diff <- DailyWeatherDiff(WeatherData.daily)

WeatherData.nonbuffer <- WeatherData.daily[(trendLen+1):nrow(WeatherData.daily),]
WeatherDataDiff.nonbuffer <- WeatherData.daily_diff[(trendLen-1):nrow(WeatherData.daily_diff),] 


## Time series regression
source("HolidayChart.R")
require(glmnet)
require(dummies)
require(doMC)
registerDoMC(cores=4)

glm <- "gaussian"
varSet <- c("DOW","weather","weatherdiff","timelag")
standardize <- "minmax"
Windowing <- TRUE
# nlambda <- 20
lambdaSeq <- 2^seq(-5,0.5,by=0.5)
parallel <- TRUE

date.pred <- as.Date("2014-07-03")
Ntrain <- 365*7
winSize <- 90
winNum <- 7

CrimeData.pred <- data.frame(DATEOCC=date.pred,TStrend=NA,TSresPred=NA,HolidayCorrection=NA)
CrimeData.pred$TStrend <- CrimeData.nonbuffer$TStrend[match(date.pred,CrimeData.nonbuffer$DATEOCC)]

# match crime data and weather data by date
DateRange <- c(CrimeData.nonbuffer$DATEOCC[1],date.pred-1)

CrimeData.hist <- subset(CrimeData.nonbuffer, DATEOCC>=DateRange[1]&DATEOCC<=DateRange[2])
WeatherData.hist <- subset(WeatherData.nonbuffer, Date>=DateRange[1]&Date<=DateRange[2])
WeatherDataDiff.hist <- subset(WeatherDataDiff.nonbuffer, Date>=DateRange[1]&Date<=DateRange[2])

selectData <- VariableSet(varSet,CrimeData.hist,WeatherData.hist,WeatherDataDiff.hist,glm)
X <- selectData$X
y <- selectData$y
CrimeData.hist2 <- selectData$crimeData

# pinpoint the training time range
startDate.train <- date.pred-Ntrain
endDate.train <- date.pred-1
dateSeq.train <- seq.Date(startDate.train,endDate.train,by=1)

if (Windowing){
  dateWindow <- HistDateWindows(dateSeq.train,date.pred,windowSize=winSize,windowNum=winNum,interval=365.25,dir="backward")
  idx.tr <- CrimeData.hist2$DATEOCC %in% dateWindow$histDates
}else{
  # use all training data
  idx.tr <- CrimeData.hist2$DATEOCC %in% dateSeq.train
}

X.train_raw <- X[idx.tr,]
y.train <- y[idx.tr]

CrimeData.test <- subset(CrimeData.nonbuffer, DATEOCC>=date.pred-7&DATEOCC<date.pred)
WeatherData.test <- subset(WeatherData.nonbuffer, Date==date.pred)
WeatherDataDiff.test <- subset(WeatherDataDiff.nonbuffer, Date==date.pred)
selectData <- VariableSet2(varSet,CrimeData.test,WeatherData.test,WeatherDataDiff.test)
X.test_raw <- selectData

scaling.train <- Standardization(X.train_raw,X.train_raw,standardize,varSet,glm)    
scaling.test <- Standardization(X.train_raw,X.test_raw,standardize,varSet,glm)
X.train <- scaling.train$scaledData
X.test <- scaling.test$scaledData
scalingflag <- scaling.test$flag

#   cvfit <- cv.glmnet(as.matrix(X.train),as.vector(y.train),family=glm,standardize=scalingflag,nlambda=nlambda,parallel=parallel)       
cvfit <- cv.glmnet(as.matrix(X.train),as.vector(y.train),family=glm,standardize=scalingflag,lambda=lambdaSeq,parallel=parallel)   
fit.lasso <- glmnet(as.matrix(X.train),as.vector(y.train),family=glm,lambda=cvfit$lambda.min,standardize=scalingflag) 

y_hat.test <- predict(fit.lasso,newx=as.matrix(X.test),type="response")    

CrimeData.pred$TSresPred[CrimeData.pred$DATEOCC==date.pred] <- y_hat.test

# Compenstate holiday cases
H_indicator<- holidays(date.pred)
if (H_indicator != 0){
  if (H_indicator==1 & format(date.pred,"%m-%d")!="01-01"){
    # Do not compensate when the new year observation is not on Jan 1st
    Correction <- 0
  }
  else{
    # "back-predict" past few years' holidays
    idx.holiday <- (CrimeData.hist2$HOLIDAY == H_indicator)
    X.holiday_raw <- X[idx.holiday,]
    y.holiday <- CrimeData.hist2$INC_CNT[idx.holiday]-CrimeData.hist2$TStrend[idx.holiday]
    
    scaling.holiday<- Standardization(X.train_raw,X.holiday_raw,standardize,varSet,glm)
    X.holiday <- scaling.holiday$scaledData
    
    y_hat.holiday <- predict(fit.lasso,newx=as.matrix(X.holiday),type="response")
    Correction <- mean(y.holiday-y_hat.holiday)
  }    
}else{
  # non-holiday days: no correction
  Correction <- 0
}  

CrimeData.pred$HolidayCorrection[CrimeData.pred$DATEOCC==date.pred] <- Correction

CrimeData.pred <- within(CrimeData.pred, TSpred <- TSresPred+TStrend+HolidayCorrection)

# upload the results
Table <- 'X_PRED_STAT4'
fetchString <- paste0("SELECT * FROM ",Table, " WHERE DATEOCC=TO_DATE('",date.pred, "', 'yyyy-mm-dd')")
existFlag <- dbGetQuery(conn,fetchString)
if (existFlag){
  updateString <- paste0("UPDATE ", Table," SET PRED_VALUE=",CrimeData.pred$TSpred,
                         " WHERE DATEOCC=TO_DATE('",date.pred, "', 'yyyy-mm-dd')")
}else{
  updateString <- paste0("INSERT INTO ", Table," VALUES (TO_DATE('",date.pred, "', 'yyyy-mm-dd'), ",CrimeData.pred$TSpred,")")
}

# sqlString <- paste0("MERGE INTO ", Table," A USING (SELECT PRED_DATE from dual) B ON (A.DATE = B.DATE)
# WHEN MATCHED THEN UPDATE SET PRED =", CrimeData.pred$TSpred, 
#  "WHEN NOT MATCHED THEN INSERT (PRED_DATE, PRED_VAL) VALUES (",date.pred, ", ", CrimeData.pred$TSpred,")")

dbSendUpdate(conn,updateString)

# Close connection
dbDisconnect(conn) 
