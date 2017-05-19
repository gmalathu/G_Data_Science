##Python Data Loading Troubleshooting WorkFlow/Train of Thought

#import required packages
import pandas as pd
import datadotworld as ddw
import seaborn as sns
from datetime import date
import numpy as np

%matplotlib inline

pd.__version__
pd.show_versions(as_json=False)

#Leverage data.world library/sdk to load RidesA, RidesB & Weather files

#Rides_DataA = ddw.load_dataset('ride-austin/Ride-Austin-june6-april13')
#Rides_DataB = ddw.load_dataset('ride-austin/Rides_DataB_Updated')
#Weather_Data = ddw.load_dataset('ride-austin/austin-weather-june2-2016-april-13-2017')
#Encountering 'ValueError'

#import pandas as pd
#below reads in 'Rides_DataA', 'Rides_DataB', 'Weather_Data'
Rides_DataA = pd.read_csv('https://query.data.world/s/9odnfomyqkvvx7fqyvas0lpnj')
Rides_DataB = pd.read_csv('https://query.data.world/s/ainysu879o2cu6bxpv679rzwq')
Weather_Data = pd.read_csv('https://query.data.world/s/2ew3liym28erkp5uvq6qid6s2')


#Code for splitting Rides_DataA large csv file into smaller chunks

for i,chunk in enumerate(pd.read_csv('Rides_DataA.csv', chunksize=100000)):
    chunk.drop(chunk.columns[[0]],axis=1)
    chunk.to_csv('Rides_DataA_{}.csv'.format(i), chunksize=100000, columns=[
    'RIDE_ID',
    'started_on',
    'created_date',
    'updated_date',
    'completed_on',
    'distance_travelled',
    'end_location_lat',
    'end_location_long',
    'driver_rating',
    'rider_rating',
    'active_driver_id',
    'charity_id',
    'requested_car_category',
    'free_credit_used',
    'surge_factor',
    'start_location_long',
    'start_location_lat',
    'rider_id',
    'round_up_amount',
    'promocode_redemption_id',
    'driver_reached_on'
    ])
	
#Code for splitting Rides_DataB large csv file into smaller chunks
#This was required because of data load performance considerations.

for i,chunk in enumerate(pd.read_csv('Rides_DataB_updated.csv', chunksize=100000)):
    chunk.drop(chunk.columns[[0]],axis=1)
    chunk.to_csv('Rides_DataB_{}.csv'.format(i), chunksize=100000, columns=[
'RIDE_ID',
'base_fare',
'total_fare',
'rate_per_mile',
'rate_per_minute',
'time_fare',
'driver_accepted_on',
'esimtated_time_arrive',
'tipped_on',
'tip',
'driving_time_to_rider',
'dispatch_location_lat',
'dispatch_location_long',
'driving_distance_to_rider',
'dispatched_on',
'status',
'driver_id',
'car_id',
'color',
'make',
'model',
'year',
'car_categories_bitmask',
'rating'
])

#Code for splitting Weather large csv file into smaller chunks

for i,chunk in enumerate(pd.read_csv('Weather_Data.csv', chunksize=100000)):
    chunk.drop(chunk.columns[[0]],axis=1)
    chunk.to_csv('Weather_Data_{}.csv'.format(i), chunksize=100000, columns=[
'RIDE_ID',
'HOURLYVISIBILITY',
'HOURLYDRYBULBTEMPC',
'HOURLYRelativeHumidity',
'HOURLYWindSpeed',
'HOURLYWindDirection',
'HOURLYPrecip',
'DAILYMaximumDryBulbTemp',
'DAILYMinimumDryBulbTemp',
'DAILYDeptFromNormalAverageTemp',
'DAILYAverageRelativeHumidity',
'DAILYSunrise',
'DAILYSunset',
'DAILYPrecip',
'DAILYAverageWindSpeed',
'DAILYPeakWindSpeed'
])

#This is a workaround for out of memory errors received. Reading in files of smaller
#chunks than the original 3 x 1.5M row datasets. Once all files have been read in, it
#requires less memory/resources to append and then join the resulting 3 files to each
#other to create dataset(s) for analysis.
#Read the split CSVs into dataframes and append them

Rides_Data_0 = pd.read_csv('Rides_DataA_0.csv', index_col=0, low_memory=False)
Rides_Data_1 = pd.read_csv('Rides_DataA_1.csv', index_col=0, low_memory=False)
Rides_Data_2 = pd.read_csv('Rides_DataA_2.csv', index_col=0, low_memory=False)
Rides_Data_3 = pd.read_csv('Rides_DataA_3.csv', index_col=0, low_memory=False)
Rides_Data_4 = pd.read_csv('Rides_DataA_4.csv', index_col=0, low_memory=False)
Rides_Data_5 = pd.read_csv('Rides_DataA_5.csv', index_col=0, low_memory=False)
Rides_Data_6 = pd.read_csv('Rides_DataA_6.csv', index_col=0, low_memory=False)
Rides_Data_7 = pd.read_csv('Rides_DataA_7.csv', index_col=0, low_memory=False)
Rides_Data_8 = pd.read_csv('Rides_DataA_8.csv', index_col=0, low_memory=False)
Rides_Data_9 = pd.read_csv('Rides_DataA_9.csv', index_col=0, low_memory=False)
Rides_Data_10 = pd.read_csv('Rides_DataA_10.csv', index_col=0, low_memory=False)
Rides_Data_11 = pd.read_csv('Rides_DataA_11.csv', index_col=0, low_memory=False)
Rides_Data_12 = pd.read_csv('Rides_DataA_12.csv', index_col=0, low_memory=False)
Rides_Data_13 = pd.read_csv('Rides_DataA_13.csv', index_col=0, low_memory=False)
Rides_Data_14 = pd.read_csv('Rides_DataA_14.csv', index_col=0, low_memory=False)

frames = [Rides_Data_0
,Rides_Data_1
,Rides_Data_2
,Rides_Data_3
,Rides_Data_4
,Rides_Data_5
,Rides_Data_6
,Rides_Data_7
,Rides_Data_8
,Rides_Data_9
,Rides_Data_10
,Rides_Data_11
,Rides_Data_12
,Rides_Data_13
,Rides_Data_14
]
Rides_DataA_All = pd.concat(frames)

#Append the Rides_DataB files

Rides_DataB_0 = pd.read_csv('Rides_DataB_0.csv', index_col=0, low_memory=False)
Rides_DataB_1 = pd.read_csv('Rides_DataB_1.csv', index_col=0, low_memory=False)
Rides_DataB_2 = pd.read_csv('Rides_DataB_2.csv', index_col=0, low_memory=False)
Rides_DataB_3 = pd.read_csv('Rides_DataB_3.csv', index_col=0, low_memory=False)
Rides_DataB_4 = pd.read_csv('Rides_DataB_4.csv', index_col=0, low_memory=False)
Rides_DataB_5 = pd.read_csv('Rides_DataB_5.csv', index_col=0, low_memory=False)
Rides_DataB_6 = pd.read_csv('Rides_DataB_6.csv', index_col=0, low_memory=False)
Rides_DataB_7 = pd.read_csv('Rides_DataB_7.csv', index_col=0, low_memory=False)
Rides_DataB_8 = pd.read_csv('Rides_DataB_8.csv', index_col=0, low_memory=False)
Rides_DataB_9 = pd.read_csv('Rides_DataB_9.csv', index_col=0, low_memory=False)
Rides_DataB_10 = pd.read_csv('Rides_DataB_10.csv', index_col=0, low_memory=False)
Rides_DataB_11 = pd.read_csv('Rides_DataB_11.csv', index_col=0, low_memory=False)
Rides_DataB_12 = pd.read_csv('Rides_DataB_12.csv', index_col=0, low_memory=False)
Rides_DataB_13 = pd.read_csv('Rides_DataB_13.csv', index_col=0, low_memory=False)
Rides_DataB_14 = pd.read_csv('Rides_DataB_14.csv', index_col=0, low_memory=False)

frames = [Rides_DataB_0
,Rides_DataB_1
,Rides_DataB_2
,Rides_DataB_3
,Rides_DataB_4
,Rides_DataB_5
,Rides_DataB_6
,Rides_DataB_7
,Rides_DataB_8
,Rides_DataB_9
,Rides_DataB_10
,Rides_DataB_11
,Rides_DataB_12
,Rides_DataB_13
,Rides_DataB_14
]
Rides_DataB_All = pd.concat(frames)

#Append the Weather data split files.

Weather_Data_0 = pd.read_csv('Weather_Data_0.csv', index_col=0, low_memory=False)
Weather_Data_1 = pd.read_csv('Weather_Data_1.csv', index_col=0, low_memory=False)
Weather_Data_2 = pd.read_csv('Weather_Data_2.csv', index_col=0, low_memory=False)
Weather_Data_3 = pd.read_csv('Weather_Data_3.csv', index_col=0, low_memory=False)
Weather_Data_4 = pd.read_csv('Weather_Data_4.csv', index_col=0, low_memory=False)
Weather_Data_5 = pd.read_csv('Weather_Data_5.csv', index_col=0, low_memory=False)
Weather_Data_6 = pd.read_csv('Weather_Data_6.csv', index_col=0, low_memory=False)
Weather_Data_7 = pd.read_csv('Weather_Data_7.csv', index_col=0, low_memory=False)
Weather_Data_8 = pd.read_csv('Weather_Data_8.csv', index_col=0, low_memory=False)
Weather_Data_9 = pd.read_csv('Weather_Data_9.csv', index_col=0, low_memory=False)
Weather_Data_10 = pd.read_csv('Weather_Data_10.csv', index_col=0, low_memory=False)
Weather_Data_11 = pd.read_csv('Weather_Data_11.csv', index_col=0, low_memory=False)
Weather_Data_12 = pd.read_csv('Weather_Data_12.csv', index_col=0, low_memory=False)
Weather_Data_13 = pd.read_csv('Weather_Data_13.csv', index_col=0, low_memory=False)
Weather_Data_14 = pd.read_csv('Weather_Data_14.csv', index_col=0, low_memory=False)

frames = [Weather_Data_0
,Weather_Data_1
,Weather_Data_2
,Weather_Data_3
,Weather_Data_4
,Weather_Data_5
,Weather_Data_6
,Weather_Data_7
,Weather_Data_8
,Weather_Data_9
,Weather_Data_10
,Weather_Data_11
,Weather_Data_12
,Weather_Data_13
,Weather_Data_14
]
Weather_Data_All = pd.concat(frames)

#This method below kicked an error when attempting to operate on the 'Rides_DataA_All' dataset created
#from the append operation.

#It worked for the 'refugee_dataset' dataframe:
#
#1
#dataframes = refugee_dataset.dataframes
#for df in dataframes:
#    	print(df)
#2		
#resources = refugee_dataset.describe()['resources']
#print('name:')
#for r in resources:
#    print(r['name'])
#print('\ntype of file:')
#for r in resources:
#    print(r['format'])
#3
#df11_15 = dataframes['refugees2011-15']
#
#print(df11_15.shape)
#df11_15.head()


dataframes = Rides_DataA_All.dataframes
for df in dataframes:
    print(df)

#Merge Weather, Rides_A & Rides_B datasets - This is an in-memory merge of three dataframes
#Note: Needed to add rsuffix='_right' option 
#AttributeError: 'DataFrame' object has no attribute 'dataframes'

joined_df = Weather_Data_All.join(Rides_DataA_All, how='inner', on='RIDE_ID', rsuffix='_right').join(Rides_DataB_All, how='inner', on='RIDE_ID', rsuffix='_right')
list(joined_df.columns.values)

Count_Row=joined_df.shape[0]
Count_Col=joined_df.shape[1]
print (Count_Row)
print (Count_Col)
#Note that the above step kicked out a Memory Error after I read in the datasets from data.world as dataframes
#using the SDK method of reading in CSV files as dataframes into Python. For now do not attempt to try both
#hese methods of reading in data in the same notebook.

#Team 4 created several variables from the data set:
1)register_date - date the rider_id was created.
2)first_date

def get_time_format(time):
    info = time.split(' ')
    year, month, day = map(int, info[0].split('-'))
    return date(year, month, day)

def get_first_time(row):
    register_date = row['created_date']
    first_date = row['completed_on']
    r_date = get_time_format(register_date)
    f_date = get_time_format(first_date)
    return 1

#Note: This operation takes a few moments to complete (to set expectations)
#lambda function passes one argument 'row' to the get_first_time() function defined above and 
#applies it the row per 'axis=1' option specified	
joined_df['get_first_time'] = joined_df.apply(lambda row: get_first_time(row), axis=1)

#Row and column stats after 'joined_df['get_first_time'] '
#Another column was added. From 61 to 62 columns.
Count_Row=joined_df.shape[0]
Count_Col=joined_df.shape[1]
print (Count_Row)
print (Count_Col)


#List new set of columns after processing 'joined_df['get_first_time']
list(joined_df)
get_col_values = joined_df['total_fare']
print (get_col_values)

#Below creates a simple pivot of total fare by rider_id (not 'RIDE_ID') so this gives
#total fare per customer

fare_df = pd.pivot_table(joined_df,
                        index = ['rider_id'],
                         values = ['total_fare'],
                        aggfunc = [np.sum, len]) #'len' is listed as an aggfunc but where is it used?
fare_df['only_local'] = 0  # test adding a new column to the pivot table
fare_df.columns = fare_df.columns.droplevel(-1)
fare_df

#Set value for new column 'only_local' using lambda function
#It appears 'len' was intended to capture length of trip
fare_df['only_local'] = fare_df['len'].apply(lambda x: 1 if x > 5 else 0)
fare_df.head()
fare_df[fare_df.only_local == 1]

#Some Business Calculations

fare_df['fare_range'] = pd.cut(fare_df['sum'], [0, 10, 25, 50, 75, 100, 125, 150, 200, 500, 750, 1000, 1500, 2000, 5000, 8000])
ax = (fare_df[fare_df.only_local == 1].groupby('fare_range').sum() / fare_df.groupby('fare_range').sum() * 100)['sum'].plot(kind='bar')
ax.set_title('% of Revenue From Austinites')
ax.set_xlabel('Customer Lifetime Value (Aggregated Total Fare)')
ax.set_ylabel('% of Austinites')

#8:38 PM 5/18/2017, this is the end of replicating Team 4's analysis thus far on my Python env. At this point we
#have enough of a grasp of how Team 4 is processing the data to understand the conclusions being made from their
#analysis.

from matplotlib.pyplot import pie, axis

fare_df['only_local'].value_counts()

colors = ['gold', 'yellowgreen', 'lightcoral', 'lightskyblue']
explode = (0, 0.1)
pie(fare_df['only_local'].value_counts(), explode=explode, labels=['Local', 'Non-Local'], colors=colors,
        autopct='%1.1f%%', shadow=True, startangle=140)
axis('equal')
ax.set_title('Local Riders vs Non-Local Riders')

fare_df['only_local']

local_mask = (fare_df['only_local'] == 1)
only_local_fares = fare_df[local_mask]
local_total_fare = only_local_fares['sum'].sum()
print local_total_fare

fare_totals = fare_df['sum'].sum()
print fare_totals

print local_total_fare / fare_totals

rider_clv = fare_df['sum'].mean()
print rider_clv

print fare_df.describe()
print ('local')
only_local_fares.describe()

