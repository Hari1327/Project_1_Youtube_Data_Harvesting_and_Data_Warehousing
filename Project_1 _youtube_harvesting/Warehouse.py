import mysql.connector


#Insert the channel data, video data and comment data into SQL
def insert_data(channel_data, video_data,comment_data):
        print("Storing the Data in SQL Warehouse")
    # Connect to MySQL server
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Hariharan@27"
        )

        if mydb.is_connected():
            print("Successfully connected to MySQL database")
            mycursor = mydb.cursor()

            # Create tables if not exist
            mycursor.execute('CREATE DATABASE IF NOT EXISTS youtube_test')
            mycursor.execute('USE youtube_test')
            mycursor.execute('''
                CREATE TABLE IF NOT EXISTS channels (
                    Channel_Name VARCHAR(255), 
                    Channel_ID VARCHAR(100), 
                    Channel_Type VARCHAR(255), 
                    Channel_views VARCHAR(100), 
                    Channel_description TEXT,
                    Channel_Status VARCHAR(50)
                )
            ''')
            mycursor.execute('''
                CREATE TABLE IF NOT EXISTS videos (
                    Channel_ID VARCHAR(100),
                    Channel_Name VARCHAR(255),
                    Video_ID VARCHAR(50), 
                    Playlist_ID VARCHAR(50), 
                    Video_name VARCHAR(255), 
                    Video_Description TEXT, 
                    Published_date DATETIME, 
                    View_count INT, 
                    Like_count INT, 
                    Favorite_count INT, 
                    Comment_count INT,
                    duration INT,
                    thumbanil VARCHAR(255),
                    caption_status VARCHAR(255)
                )
            ''')
            mycursor.execute('''
                CREATE TABLE IF NOT EXISTS comments (
                    Channel_ID VARCHAR(100),
                    Video_ID VARCHAR(50), 
                    Comment_text TEXT, 
                    Comment_ID VARCHAR(50), 
                    Author VARCHAR(100), 
                    Published_At DATETIME
                )
            ''')

            # Insert channel data
            mycursor.execute('''
                        INSERT INTO channels (Channel_Name, Channel_Id, Channel_Type, Channel_views, Channel_description, Channel_Status) 
                        VALUES (%s, %s, %s, %s, %s, %s)
                    ''', (channel_data['Channel_Name'], channel_data['Channel_Id'], ','.join(channel_data['Channel_type']), channel_data['Views'], channel_data['Channel_Description'], channel_data['Channel_Status']))
            mydb.commit()

            Playlist_Id = channel_data['Playlist_Id']
            # Insert video data
            for _, row1 in video_data.iterrows():
                video_datetime_str = row1['Published_Date'].replace('T', ' ').replace('Z', '')
                mycursor.execute('''
                    INSERT INTO videos (Channel_Id,Channel_Name, Video_ID, Playlist_ID, Video_name, Video_Description, Published_date, View_count, Like_count, Favorite_count, Comment_count, duration, thumbanil, caption_status) 
                    VALUES (%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s)
                ''', (channel_data['Channel_Id'],channel_data['Channel_Name'],row1['Video_Id'],Playlist_Id, row1['Video_name'], row1['Description'], video_datetime_str, row1['Views'], row1['Likes'], row1['Favorite_Count'], row1['Comments'],row1['Duration'],row1['Thumbnail'],row1['Caption_Status']))
                mydb.commit()

            # Insert comment data
            for _, row2 in comment_data.iterrows():
                cmt_datetime_str = row2['Comment_Published'].replace('T', ' ').replace('Z', '')
                mycursor.execute("INSERT INTO comments (Channel_Id, Video_ID, Comment_text, Comment_ID, Author, Published_at) VALUES ( %s, %s, %s, %s, %s, %s)",
                          (channel_data['Channel_Id'],row1['Video_Id'], row2['Comment_Text'], row2['Comment_Id'], row2['Comment_Author'], cmt_datetime_str))
                mydb.commit()

            
            print("Data inserted successfully")
