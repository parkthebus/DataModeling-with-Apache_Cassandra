#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 

# In[15]:


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[62]:


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
total_file_path_list = [f for f in total_file_path_list if '.ipynb_checkpoints' not in f]
total_file_path_list


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[17]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 

# for every filepath in the file path list 
for f in file_path_list:
# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
# extracting each data row one by one and append it        
        for line in csvreader:
#             print(f'\tline : {line}')
            full_data_rows_list.append(line)  


# In[18]:



# uncomment the code below if you would like to get total number of rows 
print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
print(full_data_rows_list[:2])

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',    'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''): continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[5]:


# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# In[ ]:





# I tried to do on alternative way to process csv files (pandas)
# -----

# In[89]:


dataframe_list = []
for num,f in enumerate(total_file_path_list):
    df = pd.read_csv(f)
    dataframe_list.append(df)


# In[90]:


all_dataframe = pd.concat(dataframe_list)
all_dataframe = all_dataframe.loc[all_dataframe['artist'].isna()==False]
all_dataframe = all_dataframe[['artist','firstName','gender','itemInSession','lastName','length', 'level','location','sessionId','song','userId']]
all_dataframe.head(1)


# In[91]:


all_dataframe.to_csv('event_datafile_new.csv', mode='w', index=False, encoding='utf-8')


# In[93]:


pd.read_csv(file).head(1)


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# 
# ## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
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
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# ## Begin writing your Apache Cassandra code in the cells below

# #### Creating a Cluster

# In[19]:


# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
try: 
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect() # To establish connection and begin executing queries, need a session
except Exception as e:
    print(e)


# #### Create Keyspace

# In[20]:


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

# In[21]:


# TO-DO: Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)


# ### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

# ## Create queries to ask the following three questions of the data
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

# In[101]:


try:
    session.execute("CREATE TABLE IF NOT EXISTS music                     (artist text, item_in_session int, length float, sessionid int, title text, PRIMARY KEY (sessionid, item_in_session))")
except Exception as e:
    print(e)


# In[102]:


try:
    session.execute("CREATE TABLE IF NOT EXISTS artist_and_user                     (artist text, first_name text, item_in_session int, last_name text,                    level varchar, sessionid int, title text, userid int, PRIMARY KEY (userid, sessionid))")
except Exception as e:
    print(e)


# In[103]:


try:
    session.execute("CREATE TABLE IF NOT EXISTS users                     (first_name text, gender varchar, last_name text,                    level varchar, location text, title text, userid int, PRIMARY KEY (title, first_name, last_name))")
except Exception as e:
    print(e)


# In[88]:


pd.read_csv(file).head(1)


# In[104]:


# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## TO-DO: Assign the INSERT statements into the `query` variable
        query1 = "INSERT INTO music             (artist, item_in_session, length, sessionid, title)"
        query1 = query1 + "VALUES (%s, %s, %s, %s, %s)"
        
        query2 = "INSERT INTO artist_and_user             (artist, first_name, item_in_session, last_name, level, sessionid, title, userid)"
        query2 = query2 + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        
        query3 = "INSERT INTO users             (first_name, gender, last_name, level, location, title, userid)"
        query3 = query3 + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        
        
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query1, (line[0],int(float(line[3])),float(line[5]),int(float(line[8])),line[9]))
        session.execute(query2, (line[0],line[1],int(float(line[3])),line[4],line[6],int(float(line[8])),line[9],int(float(line[10]))))
        session.execute(query3, (line[1],line[2],line[4],line[6],line[7],line[9],int(float(line[10]))))


# #### Do a SELECT to verify that the data have been inserted into each table

# In[105]:


## TO-DO: Add in the SELECT statement to verify the data was entered into the table
try:
    rows = session.execute("SELECT * FROM music")
except Exception as e:
    print(e)

for row in rows[:5]:
    print(row.artist, ' / ', row.item_in_session, ' / ', row.length, ' / ', row.sessionid, ' / ', row.title)


# In[106]:


## TO-DO: Add in the SELECT statement to verify the data was entered into the table
try:
    rows = session.execute("SELECT * FROM artist_and_user")
except Exception as e:
    print(e)

for row in rows[:5]:
    print(row.artist, ' / ', row.first_name, ' / ', row.item_in_session,           ' / ', row.last_name, ' / ', row.level, ' / ', row.sessionid, ' / ', row.title, ' / ', row.userid)


# In[107]:


## TO-DO: Add in the SELECT statement to verify the data was entered into the table
try:
    rows = session.execute("SELECT * FROM users")
except Exception as e:
    print(e)

for row in rows[:5]:
    print(row.first_name, ' / ', row.gender, ' / ', row.last_name, ' / ', row.level, ' / ', row.location, ' / ', row.title, ' / ', row.userid)


# ### COPY AND REPEAT THE ABOVE THREE CELLS FOR EACH OF THE THREE QUESTIONS

# In[109]:


## TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4
try:
    rows = session.execute("SELECT artist, title, length FROM music WHERE sessionid=338 AND item_in_session=4")
except Exception as e:
    print(e)

for row in rows:
    print(row.artist, '|', row.title, '|', row.length)
                    


# In[110]:


## TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182
try:
    rows = session.execute("SELECT artist, title, first_name, last_name FROM artist_and_user WHERE userid=10 AND sessionid=182")
except Exception as e:
    print(e)

for row in rows:
    print(row.artist,'|', row.title,'|', row.first_name, '|', row.last_name)


# In[111]:


## TO-DO: Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
try:
    rows = session.execute("SELECT first_name, last_name FROM users WHERE title='All Hands Against His Own'")
except Exception as e:
    print(e)

for row in rows:
    print(row.first_name, '|', row.last_name)

                    


# In[ ]:





# In[ ]:





# ### Drop the tables before closing out the sessions

# In[112]:


## TO-DO: Drop the table before closing out the sessions
try:
    session.execute('DROP TABLE IF EXISTS music')
except Exception as e:
    print(e)
try:
    session.execute('DROP TABLE IF EXISTS artist_and_user')
except Exception as e:
    print(e)
try:
    session.execute('DROP TABLE IF EXISTS users')
except Exception as e:
    print(e)


# ### Close the session and cluster connection¶

# In[113]:


session.shutdown()
cluster.shutdown()


# In[ ]:





# In[ ]:




