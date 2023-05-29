# Youtube-Data-Harvesting

This project aims to harvest and analyze data from YouTube using the YouTube Data API, store it in MongoDB, and migrate it to a PostgreSQL database for further analysis and through querying we can analyze the youtube channel . The project consists of several code files and functionalities.

Prerequisites:
Before running the scripts, make sure you have the following dependencies installed:

Python installed on your system
Python 3.x
Access to the YouTube Data API (API key)
MongoDB and PostgreSQL databases set up and running 

Install the following packages in the python environment
-->pymongo package (for MongoDB interaction)
-->psycopg2 package (for PostgreSQL interaction)
-->pandas package (for data manipulation and analysis)
--?streamlit package (for User Interface and interactive data visualization)
-->Google Client Library for YouTube Data API for data retrieval from youtube

You can install the required Python packages using pip:

-->pip install pymongo 
-->pip install psycopg2 
-->pip install pandas 
-->pip install streamlit 
-->pip install google-api-python-client


The Project consist of the following files:

### 1. DataHarvesting.py

This file contains the code for Scrapping data from YouTube using the YouTube Data API v3. It provides functions to generate output in JSON format and upload the data to MongoDB.

### 2. MongoConnection.py

This file handles the connection and interaction with the MongoDB database. It includes functions to establish a connection, upload data to MongoDB.

### 3. mongo_to_sql.py

This file contains functions to convert the data stored in MongoDB ,like channel information, playlists, comments, and videos into DataFrames.

### 4. df_to_sql_table.py

This file handles the creation of tables in the PostgreSQL database and the insertion of data from pandas DataFrames.To create tables for channels, playlists, videos, and comments.

### 5. channel_analysis.py

This file contains functions to perform data analysis on the migrated data in PostgreSQL. It includes functions to answer specific questions about the data, such as retrieving video and channel names, counting videos per channel, finding the most viewed videos, etc.

### 6. streamgui.py

This file implements a graphical user interface (GUI) using Streamlit. The GUI allows users to interact with the project functionalities. It provides options to retrieve data from YouTube, upload it to MongoDB, migrate it to PostgreSQL, view the migrated data, and perform selective data analysis.

Contributing
Contributions are welcome! If you have any suggestions, improvements, or bug fixes, please create an issue or submit a pull request.

License
This project is Un-Licensed
Feel free to use, modify, and distribute this project as needed.

Acknowledgements
This project utilizes the YouTube Data API to scrape YouTube data. Make sure to comply with YouTube's terms of service and API usage policies.
If you have any questions or need assistance, please don't hesitate to contact me.


