import configparser
from datetime import datetime
import pandas as pd
from pyspark.sql.functions import isnan, when, count, col ,udf, monotonically_increasing_id, year, month
import datetime as dt
from pyspark.sql.types import StringType as StringType, IntegerType as IntegerType, TimestampType, DateType
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import *

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
config = configparser.ConfigParser()
config.read('dl.cfg')
AWS_ACCESS_KEY_ID = config['default']['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = config['default']['AWS_SECRET_ACCESS_KEY']
os.environ['AWS_ACCESS_KEY_ID'] =  config['default']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['default']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    create spark_session and lauch spark in aws
    
    
    """
    spark = SparkSession.builder\
        .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .config("spark.hadoop.fs.s3a.access.key",AWS_ACCESS_KEY_ID)\
        .config("spark.hadoop.fs.s3a.secret.key",AWS_SECRET_ACCESS_KEY)\
        .enableHiveSupport().getOrCreate()
                     
    return spark

#def convert_to_datetime(date):
    
   # if date is not None:
    #    return pd.Timestamp('1960-1-1')+pd.to_timedelta(date, unit='D')
    
    
def Process_immigration_data(spark, immigration_data, output_data):
     #load immigration data             
    df_immigration = pd.read_sas(immigration_data, format='sas7bdat')
    
    #create fact table
    fact_immigration = df_immigration[['cicid', 'i94yr', 'i94mon', 'i94port', \
                                              'arrdate', 'i94addr', 'depdate']]
    fact_immigration= fact_immigration.rename(columns={"cicid" : "cicid" ,\
               "i94yr": "year",\
                "i94mon" : "month",\
                "i94port" : "Port_of_admission",\
                "arrdate" : "arrival_date",\
                "i94addr" : "state_code",\
                 "depdate" : "departure_date"})
    #fix the type of columns
    fact_immigration.dropna(inplace=True)
    fact_immigration = fact_immigration.astype({'year':'int'})
    fact_immigration = fact_immigration.astype({'month':'int'})
# convert dates to datetime
    fact_immigration['arrival_date']= pd.to_datetime(fact_immigration['arrival_date']).dt.floor('T')
    fact_immigration['departure_date']= pd.to_datetime(fact_immigration['departure_date']).dt.floor('T')
    #parquet the data
    fact_immigration = fact_immigration.to_csv("fact_immigration")
    
    fact_immigration = spark.read.option("header","true").csv("fact_immigration")

    fact_immigration.write.parquet(output_data + "fact_immigration/", mode = "overwrite")
     

    #Extract visa_dimension
    dim_visa=df_immigration[['cicid', 'i94mode', 'i94visa', 'airline', 'fltno', 'visatype']]
    dim_visa = dim_visa.rename(columns={"cicid" : "cicid" ,\
               "i94mode": "mode_of_transportation",\
                "i94visa" : "visa",\
                "fltno" : "flight_number",\
                "visatype" : "visa_type"})
    
    dim_visa.dropna(inplace=True)
    
    dim_visa.to_csv("dim_visa")
    
    dim_visa = spark.read.option("header","true").csv("dim_visa")

    dim_visa.write.parquet(output_data + "dim_visa/", mode = "overwrite")
     
    
        
    #Extract Flag_dimension


    dim_flag = df_immigration[['cicid', 'entdepa', 'entdepd', 'entdepu', 'matflag']]
    
    dim_flag = dim_flag.rename(columns={"cicid" : "cicid" ,\
               "entdepa": "Arrival_Flag",\
                "entdepd" : "Departure_Flag",\
                "entdepu" : "Update_Flag",\
                "matflag" : "Match_Flag"})
    
    dim_flag.dropna(inplace=True)
    
    dim_flag.to_csv("dim_flag")
    
    dim_flag = spark.read.option("header","true").csv("dim_flag")

    dim_flag.write.parquet(output_data + "dim_flag/", mode = "overwrite")
     

    

    
    #Extract immigrant dimension

    dim_immigrant = df_immigration[['cicid', 'i94cit', 'i94res', 'i94bir', 'biryear', 'gender']]
    dim_immigrant= dim_immigrant.rename(columns={"cicid" : "cicid" ,\
               "i94cit": "citizenship_code",\
                "i94res" : "residence_code",\
                "i94bir" : "birthday",\
                "biryear" : "birthday_year"})
    
    dim_immigrant.dropna(inplace=True)
    dim_immigrant['birthday_year'] = dim_immigrant['birthday_year'].astype('int')

    
    dim_immigrant.to_csv("dim_immigrant")
    
    dim_immigrant = spark.read.option("header","true").csv("dim_immigrant")

    dim_immigrant.write.parquet(output_data + "dim_immigrant/", mode = "overwrite")
    


def Process_demographics_data(spark, demographics_data, output_data):
    #load demographics csv
    df_demographics = pd.read_csv(demographics_data, sep=';')
    #clean it: rename the columns and drop the missing values
    df_demographics= df_demographics.rename(columns={"State Code": "State_Code",\
                                                     "Median Age": "Median_Age",\
                                                       "Male Population": "Male_Population",\
                                                         "Female Population" : "Female_Population",\
                                                           "Total Population" : "Total_Population",\
                                                             "Number of Veterans" : "Number_of_Veterans",
                                                             "Foreign-born" : "Foreign_born",\
                                                               "Average Household Size": "Average_Household_Size"})
    
    df_demographics.dropna(inplace=True)
    
    df_demographics['Male_Population'] = df_demographics['Male_Population'].astype('int')
    df_demographics['Female_Population'] = df_demographics['Female_Population'].astype('int')
    df_demographics['Total_Population'] = df_demographics['Total_Population'].astype('int')

    
    #load data in dim_city
    dim_city = df_demographics[["City","State","State_Code","Race"]]
    
    #parquet Data
    dim_city.to_csv("dim_city")
    
    dim_city = spark.read.option("header","true").csv("dim_city")
    
    dim_city = dim_city.write.parquet(output_data+"dim_city/", mode = "overwrite")
     
    
    
    #dim_Population 
    #load dim_populatio data
    dim_Population=df_demographics[['Median_Age', 'Male_Population', 'Female_Population',\
      'Total_Population', 'Number_of_Veterans', 'Foreign_born',\
       'Average_Household_Size', 'State_Code', "Race"]]
    
    #parquet data
    dim_Population.to_csv("dim_Population")
    
    dim_Population=spark.read.option("header","true").csv("dim_Population")
    dim_Population.write.parquet(output_data + "dim_Population/", mode = "overwrite")

    
     
    

def  Process_temperature_data(spark, temperature_data,output_data):
  #load temperature data
    temperature_df = pd.read_csv(temperature_data)
    dim_temperature=temperature_df[["dt","AverageTemperature","AverageTemperatureUncertainty","City","Country"]]
    #drop th emissing values
    dim_temperature.dropna(inplace=True)
    #filter the country
    dim_temperature = dim_temperature.loc[dim_temperature['Country'] == 'United States']
    #fix the columns type
    dim_temperature['dt']= pd.to_datetime(dim_temperature['dt'])
    dim_temperature['year'] = pd.DatetimeIndex(dim_temperature['dt']).year
    dim_temperature['month'] = pd.DatetimeIndex(dim_temperature['dt']).month

    
    #parquet the data
    dim_temperature.to_csv("dim_temperature")
    
    dim_temperature=spark.read.option("header","true").csv("dim_temperature")

    dim_temperature=dim_temperature.write.parquet(output_data+"dim_temperature/", mode = "overwrite")
        
     
    
def main():
    
    """
    implement all function   here 
    """      
    spark = create_spark_session()
    immigration_data='../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    demographics_data='us-cities-demographics.csv'
    temperature_data= '../../data2/GlobalLandTemperaturesByCity.csv'
    
    output_data = "s3a://aws-logs-086370891584-us-west-2/elasticmapreduce/"
    
    Process_immigration_data(spark, immigration_data, output_data)
    Process_demographics_data(spark, demographics_data, output_data)
    Process_temperature_data(spark, temperature_data, output_data)
    
    
    
if __name__ == "__main__":
    main()
    
    