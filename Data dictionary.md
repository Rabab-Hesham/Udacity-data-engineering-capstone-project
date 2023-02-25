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


# Dimension tables
our schema consists of 6 dimension tables:
1- Three dimension tables created from I94 Immigration Data: (dim_visa, dim_flag, and dim_immigrant)
3 - Two dimension tables extracted from U.S. City Demographic Data (dim_city and dim_Population)
2- dim_temperature: it's data extracted from world Temperature Data

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
