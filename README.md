# Project_1_Youtube_Data_Harvesting_and_Data_Warehousing
Data Analytics Project
Overview
This project aims to extract data from a specific YouTube channel using its Channel ID and store the extracted data in a SQL database. The main objective is to facilitate data analysis and reporting by systematically organizing and storing the channel's data. This includes retrieving video details, statistics, and other relevant metadata to provide insights into the channel's performance and content trends.

Features
Channel Data Extraction: Fetch detailed information about videos from a specified YouTube channel using the Channel ID.
Data Storage: Store the extracted data in a SQL database for efficient querying and analysis.
Automated Data Pipeline: Set up an automated process to regularly update the database with the latest data from the channel.
Data Analysis Ready: Organize the data in a structured format, making it easy to perform various data analysis tasks.
Technologies Used
Python: The core programming language used for developing the data extraction and processing scripts.
YouTube Data API v3: Utilized to fetch data from the YouTube channel.
SQL Database: For storing the extracted data. SQLite is used for simplicity, but the code can be adapted to other SQL databases like MySQL or PostgreSQL.
Pandas: For data manipulation and transformation.
SQLAlchemy: An ORM (Object-Relational Mapping) tool for interacting with the SQL database.
Prerequisites
Python 3.x installed on your system.
A Google Cloud Project with YouTube Data API enabled.
API Key from the Google Developer Console.
SQLite (or any other preferred SQL database) installed and set up.
Installation
Clone the repository:

bash
Copy code
git clone https://github.com/yourusername/data-analytics-project.git
cd data-analytics-project
Install the required Python packages:

bash
Copy code
pip install -r requirements.txt
Set up your API Key:

Create a .env file in the project root and add your YouTube Data API key:

bash
Copy code
YOUTUBE_API_KEY=YOUR_API_KEY_HERE
Configure the Database:

By default, the project uses SQLite. Ensure the database file path is correctly set in the config.py file. If using another SQL database, update the connection string accordingly.

Usage
Run the Data Extraction Script:

bash
Copy code
python extract_data.py --channel_id YOUR_CHANNEL_ID
This will fetch data from the specified YouTube channel and store it in the SQL database.

Automate Data Updates:

To keep the database updated with the latest data, you can schedule the script to run at regular intervals using a task scheduler like cron (Linux) or Task Scheduler (Windows).

Project Structure
extract_data.py: Main script to extract data from the YouTube channel.
database.py: Contains functions for interacting with the SQL database.
config.py: Configuration file for database connection and API key settings.
requirements.txt: List of Python dependencies.
.env: File to store environment variables, including API key.
Contributions
Contributions are welcome! If you have any suggestions or improvements, feel free to create a pull request or open an issue.

License
This project is licensed under the MIT License. See the LICENSE file for more details.
