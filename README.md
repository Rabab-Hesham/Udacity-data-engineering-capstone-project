## Project objective:

The objective of the project is to create an ETL pipeline for I94 immigration, global temperatures and US demographics datasets to form an analytics database on immigration events over time. The resulting database will be helpful in many analytical aspects like the affect of the temperature on the number of immigrant, gender and race of immigrants etc.

# The project steps:
1- Scope the Project and Gather Data
2- Explore and Assess the Data
3- Define the Data Model
4- Run ETL to Model the Data
5- Complete Project Write Up

# Scope the Project:

Spark and pandas will be used to load the data into dataframes and explore them. Once the datacleaned, the same steps with a little modifications will be used to parquet files and stored in an output folder.
To parquet thefiles, I  prefered using pandas at first to clea the data, since I noticed tht saves a lot if time, and then converting data to spark and parquet them.

te data consists of:
- I94 Immigration Data: This data comes from the US National Tourism and Trade Office. 
- World Temperature Data: This dataset came from Kaggle. 
- U.S. City Demographic Data: This data comes from OpenSoft.


# Data Model:

star schema will be used

The Fact Table: I94 Immigration Data
All foreign visitors coming into the USA via air or sea were subject to an examination by the Customs Border Protection (CBP) officer after which they would be issued either a passport admission stamp or a small white card called the I-94 form. This form contains precisely the data which we are basing our data model on.

cicid --> Unique identifier
i94yr --> year
i94mon --> month
i94port --> Port_of_admission
arrdate --> arrival_date
i94addr --> state_code
depdate --> departure_date
## Data dictionary 

# Dimension tables
1- dim_visa

cicid --> Unique identifier
i94mode --> mode of transportation
i94visa --> visa
airline --> airline
fltno --> flight number
visatype --> visa type


2- dim_flag
cicid --> Unique identifier
entdepa --> Arrival_Flag
entdepd --> Departure_Flag
entdepu --> Update_Flag
matflag --> Match_Flag

3- dim_immigrant
cicid --> Unique identifier

i94cit --> citizenship_code
i94res --> residence_code
i94bir -->  birthday
biryear --> birthday_year
gender --> gender

4- dim_city : City ,State, State_Code, Race

5- dim_Population : Median_Age, Male_Population, Female_Population, Total_Population, Number_of_Veterans, Foreign_born'
       Average_Household_Size, State_Code, Race
       
6- dim_temperature: dt (date time), Average Temperature, Average Temperature Uncertainty, City, Country
