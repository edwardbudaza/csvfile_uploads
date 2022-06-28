import pandas as pd
from os import path
import os, sqlite3
import re
import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

 # 1. Create a SQLite database called `my_sqlite.db`


db_name = 'my_sqlite.db'

#try:
#    os.remove(db_name)
#except:
#    pass


# start db connection
db_conn = sqlite3.connect(db_name)
c = db_conn.cursor()
    

# code here

file_path = 'data/'
if path.isdir(file_path):
    all_files = os.listdir(file_path)


def read_csv(file_path):
    """
    Reads List CSV data into pandas DataFrame - and returns the DataFrame
    Input: 
        file_path - path were fiels are located
    
    """
    csv_files = list(filter(lambda f: f.endswith('.csv'), all_files))

    regex = re.compile(r'\d+')

    time_series_list = []

    for file in csv_files:
        device_id = [int(x) for x in regex.findall(file)]

        df_temp = pd.read_csv(file_path + "\\" + file)
        df_temp['device_id'] = device_id[0]
        df_temp['series_type'] = 'Network Traffic'

        time_series_list.append(df_temp)



    time_series = pd.concat(time_series_list,axis=0,ignore_index=True)    
    time_series = time_series.sort_values(by='device_id').set_index('date')
    
    return time_series


def read_text(file_path):
    """"
    Reads metat data into pandas DataFrame - and returns the DataFrame
    Input: 
        file_path - path were fiel is located
    """
    
    txt_file = list(filter(lambda f: f.endswith('.txt'), all_files))[0]
    with open(file_path + "\\" + txt_file) as f:
        data = f.read()

    js = json.loads(data)
    meta_data = pd.DataFrame(js).sort_values(by=['id']).reset_index(drop=True)
    
    return meta_data


def load_series_data(time_series_data, db_conn):
    """
    Loads Time Series data into the database table time_series
    Input:
        time_series_data - dataframe with time series data
        db_conn - database connection
    """
    
    c.execute('CREATE TABLE IF NOT EXISTS time_series (date TEXT, device_id INTEGER,series_type TEXT, value REAL)')
    
    
    time_series_data.to_sql('time_series', db_conn, if_exists='replace', index = True)
    db_conn.commit()
    
    return 'Time Series Data loaded successfully'
    
    
def load_meta_data(meta_data, db_conn):
    """
    Loads Meta data into the database table meta
    Input:
        meta_data - dataframe with meta data
        db_conn - database connection
    """
    
    c.execute('CREATE TABLE IF NOT EXISTS meta (id INTEGER,name TEXT)')
    
    
    meta_data.to_sql('meta', db_conn, if_exists='replace', index = False)
    db_conn.commit()
    return 'Meta Data loaded successfully'


def find_top_records(top, df_meta, df_time_series):
    """
    Gets top records by specifying how many records - i.e top = 5, brings top 5 records
    Input:
        top - i.e top 5 devices total network traffic by device name
        df_meta - meta data DataFrame
        df_time_series - time series DataFrame
    """
    
    data = df_meta.merge(df_time_series, left_on='id', right_on='device_id', how='inner').groupby(['name'])\
    .sum().nlargest(top,'traffic').filter(items=['traffic'])
    
    return data


def find_outliers_IQR(data):
    """
    Will calculate the outlier data points using the statistical method called interquartile range (IQR)
    Input:
        data - DataFrame with traffic data
    """
    
    q1 = data['traffic'].quantile(0.25)
    q3 = data['traffic'].quantile(0.75)
    
    IQR = q3 - q1
    
    outliers = data[((data['traffic']<(q1-1.5*IQR)) | (data['traffic']>(q3+1.5*IQR)))]
    
    return outliers



def day_total_network_traffic(date):
    """
    Gets total network traffice given a date
    Input:
        Date - Date value
    """
    total_network_traffic = pd.read_sql('SELECT SUM(trafFic) AS total_network_traffic FROM time_series WHERE \
    date = "{}" group by {}'.format(date, 'date'),db_conn)
    return total_network_traffic


def export_oddities(outliers):
    """
    exports JSON data of oddities
    """
    oddities = outliers.reset_index().to_json(orient='records')

    with open('oddities.json', 'w', encoding='utf-8') as f:
        json.dump(oddities, f)

    return 'File exported as oddities.json!'



def main():
   
    
    #3. Insert the network traffic series as is into the time_series table for each IoT device 
    #   (you can use any viable series_type descriptor)
    #time_series_data = read_csv(file_path)
    #print(load_series_data(time_series_data, db_conn) + '\n')
    
    
    # 5. Parse the meta data file and insert the data into the meta table
    #meta_data = read_text(file_path)
    #print(load_meta_data(meta_data, db_conn) + '\n')
    
    
    # 6. Write SQL to select all the records from the meta table and store it in a Pandas DataFrame
    #    called df_meta
    df_meta = pd.read_sql('SELECT * FROM meta',db_conn)
    df_time_series = pd.read_sql('SELECT * FROM time_series', db_conn)
    
    
    # 7. Write SQL to find the total network traffic on the first day of the year across all the devices
    print('\n -------- Total Network Traffic at the beginning of the year --------')
    print(day_total_network_traffic('2020-01-01'))
    
    
    # 8. Write SQL to find the top 5 devices total network traffic by device name
    # ###### Getting the Top 5 using SQL will require
    # -- Table Join between time series data and meta data using the device_id 
    # 
    # -- To get the SUM will GROUP BY device name
    # 
    # -- Will have to get the TOP 5 of the SUM
    # 
    # ###### ***** To get result will have to nest SQL ***** ##
    # ###### I decided to do all of this using a single line with pandas - this way can also be parameterised and used as a function ####
    print('\n -------- Top 5 devices total network traffic by device name --------')
    print(find_top_records(5, df_meta, df_time_series))
    
    
    #############################################################
    # finally close connection
    db_conn.close()
    
    
    #### TASK 2 ####
    
    df = df_meta.merge(df_time_series, left_on='id', right_on='device_id', how='inner')
    df = df.drop(columns=['series_type','id']).set_index('date')
    
    print('\n -------- Summary Statistcis --------')
    print(df.describe()['traffic'])
    print("""
    
    # As we can see the traffic has outliers. 
    # For instance, max traffic is 199.7 while its mean is 99.9. 
    # The mean is sensitive to outliers, but the fact that the mean is small 
    # compared to the max value idicates the max value is an outlier. Similarly, 
    # the min traffic is 0 while the mean is 99.9 with a small standard deviation of 5.9. 
    #We will explore using additional methods...
    
    """)
    
    
    box = df.boxplot(column=['traffic'], figsize=(15,7))
    
    hist = df.hist(column=['traffic'], figsize=(15,7), bins=500)
    
    print('\n Using visuals we are able to identify that the are some outliers - to investigate further we use statistical method. \n')
    
    
    ## Finding outliers using statistical methods
    
    # We will calculate the outlier data points using the statistical method called interquartile range (IQR).
    # Using the IQR, the outliers data points are the ones falling below Q1 - 1.5 IQR or above Q3 + 1.5 IQR. 
    # The Q1 is the 25th percentile and Q3 is the 75th percentile of the dataset, and IQR represents the 
    # inter quartile range calculate by Q3 minus Q1 (Q3 - Q1).
    
    outliers = find_outliers_IQR(df)

    print('\n -------- Finding outliers using statistical methods --------')
    print('Number of outliers: ' + str(len(outliers)))
    print('Total records: ' + str(len(df)))
    print('Percent of outliers: {} % '.format(int(len(outliers))/int(len(df)) * 100) )
    print('Max outlier value: ' + str(outliers.traffic.max()))
    print('Min outlier value: ' + str(outliers.traffic.min()))

    
    
    outliers.index = pd.to_datetime(outliers.index)
    fig, ax = plt.subplots(figsize=(15, 7))
    year_month_formatter = mdates.DateFormatter("%Y-%m")
    ax.xaxis.set_major_formatter(year_month_formatter)
    ax.scatter(outliers.index, outliers['traffic'])
    
    
    #### 3. What can you deduce about these oddities?
    
    # Using IQR method, we find (3210) ~ 0.9 % percent of the data is outliers, 
    # these points fall outside of the interquartile range. The minimum outlier is 0 and the maximum outlier is ~ 199.7. 
    # This agrees with the data description method. 
    
    ##### 4. Output the odd behaved devices ID and name to a JSON formatted file called `oddities.json`
    print('\n -------- Output JSON formatted file called `oddities.json` --------')
    print(export_oddities(outliers))
    
if __name__ == '__main__':
    main()











