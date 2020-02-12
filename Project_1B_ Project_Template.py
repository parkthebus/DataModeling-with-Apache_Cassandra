#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# #### Import Python packages 

# In[1]:


# Import Python packages 
import pandas as pd
import cassandra
from cassandra.cluster import Cluster
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[2]:


# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

total_file_path_list = []
# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*.csv'))
    total_file_path_list += file_path_list

# get file path list except for '*.ipynb_checkpoints' files
total_file_path_list = [f for f in total_file_path_list if '.ipynb_checkpoints' not in f]


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[3]:


# create dataframe list includes dataframes from csv files.
dataframe_list = [pd.read_csv(f) for f in total_file_path_list]
# concatenate all dataframes to one dataframe.
all_dataframe = pd.concat(dataframe_list)
# remove rows that contains NaN value on 'artist' column.
all_dataframe = all_dataframe.loc[all_dataframe['artist'].isna()==False]
# filter dataframe columns what we need.
all_dataframe = all_dataframe[['artist','firstName','gender','itemInSession','lastName','length', 'level','location','sessionId','song','userId']]
# save to 'event_datafile_new.csv' file. (no index, encoded by utf-8 format)
all_dataframe.to_csv('event_datafile_new.csv', mode='w', index=False, encoding='utf-8')


# # Part II.
# 
# ## The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 

# #### Creating a Cluster

# In[4]:


# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)
try: 
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect() # To establish connection and begin executing queries, need a session
except Exception as e:
    print(e)


# #### Create Keyspace

# In[5]:


# TO-DO: Create a Keyspace 
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


# #### Set Keyspace

# In[6]:


# TO-DO: Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)


# ### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 

# In[58]:


# Query Description: In this query, 
# I used 'sessionid' as the partition key and 'item_in_session' as my clustering key. 
# Each partition is uniquely identified by 'sessionid' 
# while 'item_in_session' was used to uniquely identify the rows within a partition to sort the data by the value of num .

query = "CREATE TABLE IF NOT EXISTS music"
query = query + "(sessionid int, item_in_session int, artist text, title text, length float, PRIMARY KEY (sessionid, item_in_session))"

try:
    session.execute(query)
except Exception as e:
    print(e)


# In[59]:


# Query Description: In this query, 
# I used 'userid' and 'sessionid' as the partition key and 'item_in_session' as my clustering key. 
# Each partition is uniquely identified by 'userid' and 'sessionid'
# while 'item_in_session' was used to uniquely identify the rows within a partition to sort the data by the value of num .

query = "CREATE TABLE IF NOT EXISTS song_playlist_session"
query = query + "(userid int, sessionid int, item_in_session int, artist text, title text, first_name text, last_name text,                 PRIMARY KEY ((userid, sessionid), item_in_session))"

try:
    session.execute(query)
except Exception as e:
    print(e)


# In[60]:


# Query Description: In this query, 
# I used 'title' as the partition key and 'userid' as my clustering key. 
# Each partition is uniquely identified by 'title' 
# while 'userid' was used to uniquely identify the rows within a partition to sort the data by the value of num .

query = "CREATE TABLE IF NOT EXISTS users"
query = query + "(title text, userid int, first_name text, last_name text, level varchar, location text, PRIMARY KEY (title, userid))"

try:
    session.execute(query)
except Exception as e:
    print(e)


# In[61]:


# for n,l in enumerate(list(all_dataframe.columns)):
#     print(f'{n} : {l} \t ({type(l)})')


# In[62]:


# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        ## TO-DO: Assign the INSERT statements into the `query` variable
        query1 = "INSERT INTO music             (sessionid, item_in_session, artist, title, length)"
        query1 = query1 + "VALUES (%s, %s, %s, %s, %s)"
        
        query2 = "INSERT INTO song_playlist_session             (userid, sessionid, item_in_session, artist, title, first_name, last_name)"
        query2 = query2 + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        
        query3 = "INSERT INTO users             (title, userid, first_name, last_name, level, location)"
        query3 = query3 + "VALUES (%s, %s, %s, %s, %s, %s)"
        
        
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query1, (int(float(line[8])),int(float(line[3])),line[0],line[9],float(line[5])))
        session.execute(query2, (int(float(line[10])),int(float(line[8])),int(float(line[3])),line[0],line[9],line[1],line[4]))
        session.execute(query3, (line[9], int(float(line[10])),line[1],line[4],line[6],line[7]))


# #### Do a SELECT to verify that the data have been inserted into each table

# In[63]:


## TO-DO: Add in the SELECT statement to verify the data was entered into the table
try:
    rows = session.execute("SELECT * FROM music")
except Exception as e:
    print(e)

for row in rows[:5]:
    print(row.sessionid, ' / ', row.item_in_session, ' / ', row.artist, ' / ', row.title, ' / ', row.length)


# In[64]:


## TO-DO: Add in the SELECT statement to verify the data was entered into the table
try:
    rows = session.execute("SELECT * FROM song_playlist_session")
except Exception as e:
    print(e)

for row in rows[:5]:
    print(row.userid, ' / ', row.sessionid, ' / ', row.item_in_session,           ' / ', row.artist, ' / ', row.title, ' / ', row.first_name, ' / ', row.last_name, ' / ')


# In[65]:


## TO-DO: Add in the SELECT statement to verify the data was entered into the table
try:
    rows = session.execute("SELECT * FROM users")
except Exception as e:
    print(e)

for row in rows[:5]:
    print(row.userid, ' / ', row.title, ' / ', row.first_name, ' / ', row.last_name, ' / ', row.level, ' / ', row.location, ' / ')


# In[66]:


## TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4
try:
    rows = session.execute("SELECT artist, title, length FROM music WHERE sessionid=338 AND item_in_session=4")
except Exception as e:
    print(e)

for row in rows:
    print(row.artist, '|', row.title, '|', row.length)
                    


# In[67]:


## TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182
try:
    rows = session.execute("SELECT artist, title, first_name, last_name FROM song_playlist_session WHERE userid=10 AND sessionid=182")
except Exception as e:
    print(e)

for row in rows:
    print(row.artist,'|', row.title,'|', row.first_name, '|', row.last_name)


# In[68]:


## TO-DO: Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
try:
    rows = session.execute("SELECT first_name, last_name FROM users WHERE title='All Hands Against His Own'")
except Exception as e:
    print(e)

for row in rows:
    print(row.first_name, '|', row.last_name)

                    


# In[50]:


# all_dataframe.loc[all_dataframe['song']=='All Hands Against His Own']


# In[ ]:





# ### Drop the tables before closing out the sessions

# In[69]:


## TO-DO: Drop the table before closing out the sessions
try:
    session.execute('DROP TABLE IF EXISTS music')
except Exception as e:
    print(e)
try:
    session.execute('DROP TABLE IF EXISTS song_playlist_session')
except Exception as e:
    print(e)
try:
    session.execute('DROP TABLE IF EXISTS users')
except Exception as e:
    print(e)


# ### Close the session and cluster connection¶

# In[70]:


session.shutdown()
cluster.shutdown()


# In[ ]:





# In[ ]:




