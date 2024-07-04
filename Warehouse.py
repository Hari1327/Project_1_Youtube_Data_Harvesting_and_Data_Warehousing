import mysql.connector
from mysql.connector import Error
import toml

def insert_data(channel_data, video_data, comment_data):
    try:
        print("Storing the Data in SQL Warehouse")
        
        # Connecting to Database
        config = toml.load("secrets.toml")

        # Extract MySQL connection details
        mysql_config = config.get('mysql', {})
        host = mysql_config.get('host')
        user = mysql_config.get('user')
        password = mysql_config.get('password')
        database = mysql_config.get('database')
        
        mydb = mysql.connector.connect(host = host, user = user, password = password, database = database)
        if mydb.is_connected():
            print("Successfully connected to MySQL database")
            mycursor = mydb.cursor()

            # Create database if not exists
            mycursor.execute('CREATE DATABASE IF NOT EXISTS youtube_test')
            mycursor.execute('USE youtube_test')

            # Create tables if not exist
            mycursor.execute('''
                CREATE TABLE IF NOT EXISTS channels (
                    Channel_Name VARCHAR(255), 
                    Channel_ID VARCHAR(100) NOT NULL, 
                    Channel_Type VARCHAR(255), 
                    Channel_views VARCHAR(100), 
                    Channel_description TEXT,
                    Channel_Status VARCHAR(50),
                    PRIMARY KEY (Channel_ID)
                )
            ''')
            mycursor.execute('''
                CREATE TABLE IF NOT EXISTS videos (
                    Channel_ID VARCHAR(100),
                    Channel_Name VARCHAR(255),
                    Video_ID VARCHAR(50) NOT NULL, 
                    Playlist_ID VARCHAR(50), 
                    Video_name VARCHAR(255), 
                    Video_Description TEXT, 
                    Published_date DATETIME, 
                    View_count INT, 
                    Like_count INT, 
                    Favorite_count INT, 
                    Comment_count INT,
                    duration INT,
                    thumbnail VARCHAR(255),
                    caption_status VARCHAR(255),
                    PRIMARY KEY (Video_ID)
                )
            ''')
            mycursor.execute('''
                CREATE TABLE IF NOT EXISTS comments (
                    Channel_ID VARCHAR(100) ,
                    Video_ID VARCHAR(50), 
                    Comment_text TEXT, 
                    Comment_ID VARCHAR(50) NOT NULL, 
                    Author VARCHAR(100), 
                    Published_At DATETIME,
                    PRIMARY KEY (Comment_ID)
                )
            ''')

            # Insert channel data
            mycursor.execute('''
                INSERT INTO channels (Channel_Name, Channel_ID, Channel_Type, Channel_views, Channel_description, Channel_Status) 
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (channel_data['Channel_Name'], channel_data['Channel_Id'], ','.join(channel_data['Channel_type']), channel_data['Views'], channel_data['Channel_Description'], channel_data['Channel_Status']))
            mydb.commit()

            Playlist_Id = channel_data['Playlist_Id']
            # Insert video data
            for _, row1 in video_data.iterrows():
                video_datetime_str = row1['Published_Date'].replace('T', ' ').replace('Z', '')
                mycursor.execute('''
                    INSERT INTO videos (Channel_ID, Channel_Name, Video_ID, Playlist_ID, Video_name, Video_Description, Published_date, View_count, Like_count, Favorite_count, Comment_count, duration, thumbnail, caption_status) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (channel_data['Channel_Id'], channel_data['Channel_Name'], row1['Video_Id'], Playlist_Id, row1['Video_name'], row1['Description'], video_datetime_str, row1['Views'], row1['Likes'], row1['Favorite_Count'], row1['Comments'], row1['Duration'], row1['Thumbnail'], row1['Caption_Status']))
                mydb.commit()

            # Insert comment data
            for _, row2 in comment_data.iterrows():
                cmt_datetime_str = row2['Comment_Published'].replace('T', ' ').replace('Z', '')
                mycursor.execute('''
                    INSERT INTO comments (Channel_ID, Video_ID, Comment_text, Comment_ID, Author, Published_At) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                ''', (channel_data['Channel_Id'], row2['Video_Id'], row2['Comment_Text'], row2['Comment_Id'], row2['Comment_Author'], cmt_datetime_str))
                mydb.commit()

            print("Data inserted successfully")
    
    except Error as e:
        print(f"Error: {e}")
    
    finally:
        if mydb.is_connected():
            mycursor.close()
            mydb.close()
            print("MySQL connection is closed")
