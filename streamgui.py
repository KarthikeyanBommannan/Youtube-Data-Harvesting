import streamlit as st
from DataHarvesting import generate_output
from MongoConnection import upload_to_mongodb
from MongoConnection import migrate_to_postgresSQL
from mongo_to_sql import channel_info_df
from mongo_to_sql import playlist_to_df
from mongo_to_sql import comments_to_df
from mongo_to_sql import video_to_df
from df_to_sql_table import channel_table_creation
from df_to_sql_table import playlist_table_creation
from df_to_sql_table import video_table_creation
from df_to_sql_table import comment_table_creation
from channel_analysis import *

st.set_page_config( page_title="Youtube Data Harvesting", page_icon= "📄",layout="wide")
# st.markdown('<div class="main">', unsafe_allow_html=True)
# st.markdown("""<style>.main { max-width: 1000px;margin: 1 auto;}</style>""",unsafe_allow_html=True,)
# st.markdown('</div>', unsafe_allow_html=True)
st.title(":green[Youtube Data Harvesting]")


def main():
    left_column,right_column = st.columns(2)
    with left_column: 
        API_Key  = st.text_input('Enter the Valid API Key: ', type="password")
        channel_ids = st.text_input('Enter the Channel ID: ')
        col1,col2,col3 = st.columns(3)
        if col1.button("Get Json"):
            try:
                json_output = generate_output(API_Key,channel_ids)
                with right_column:
                    st.json(json_output)
            except:
                st.error("An error occurred. Please check your API Key or Channel ID.")
        if col2.button("Get Data"):
            try:
                data_output = generate_output(API_Key, channel_ids)
                data = ""

                def convert_dict_to_text(dictionary, indent=0):
                    nonlocal data
                    for key, value in dictionary.items():
                        if isinstance(value, dict):
                            data += f"{'    ' * indent}{key}:\n"
                            convert_dict_to_text(value, indent + 1)
                        else:
                            data += f"{'    ' * indent}{key}: {value}\n"

                convert_dict_to_text(data_output)
                with right_column:
                    st.text(data)
            except Exception as e:
                st.error(f"An error occurred. Please check your API Key or Channel ID.,{str(e)}")

        if col3.button("To MongoDB"):
            try:
                upload_to_mongodb(API_Key,channel_ids)
                with right_column:
                    st.success("Successfully Uploaded to MongoDB")
            except:
                st.error("An error occurred, While Uploading to MongoDB")   
    with left_column:
        st.title(":green[MongoDB to PGSQL]")
        available_doc_names = migrate_to_postgresSQL()
        option = st.selectbox("Select Channel Name to be Migrate with PGSQL",available_doc_names)
        st.write('The selected Channel is: ',option)
        
    with left_column:
        col4,col5 = st.columns(2)
        if col4.button("View Data"):
            with right_column:
                if option:
                    try:
                        data = channel_info_df()
                        output_data = data[data["Channel_Name"] == option]
                        if not output_data.empty:
                            st.write("Table: :red[Channel]")
                            st.write(output_data)
                    except Exception as e:
                        st.error("Selected Channel not found in the DataFrame")
                if option:
                    try:
                        data1 = playlist_to_df()
                        output_data1 = data1[data1["Channel_Name"] == option]
                        if not output_data1.empty:
                            st.write("Table: :red[Playlist]")
                            st.write(output_data1)
                    except Exception as e:
                        st.error("An error occurred: " + str(e))
                if option:
                    try:
                        data2 = video_to_df()
                        output_data2 = data2[data2["Channel_Name"] == option]
                        if not output_data2.empty:
                            st.write("Table: :red[Video]")
                            st.write(output_data2)
                    except Exception as e:
                        st.error("An error occurred: " + str(e))
                if option:
                    try:
                        data3 = comments_to_df()
                        output_data3 = data3[data3["Channel_Name"] == option]
                        if not output_data3.empty:
                            st.write("Table: :red[Comments]")
                            st.write(output_data3)
                    except Exception as e:
                        st.error("An error occurred: " + str(e))
        if col5.button("Complete Database Migrate"):
            with right_column:
                try:
                    channel_table_creation()
                except Exception as e:
                    st.error("An error occurred: " + str(e))
                try:
                    playlist_table_creation()
                except Exception as e:
                    st.error("An error occurred: " + str(e))
                try:
                    video_table_creation()
                except Exception as e:
                    st.error("An error occurred: " + str(e))
                try:
                    comment_table_creation()
                except Exception as e:
                    st.error("An error occurred: " + str(e))
                st.success("Successfully Migrated to Postgres")                       
    with left_column:
        st.write("SELECTIVE 10 QUESTIONS")
        questions = st.selectbox("Select the Question",('Tap to view',
               '1. What are the names of all the videos and their corresponding channels?',
                '2. Which channels have the most number of videos, and how many videos do they have?',
                '3. What are the top 10 most viewed videos and their respective channels?',
                '4. How many comments were made on each video, and what are their corresponding video names?',
                '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                '8. What are the names of all the channels that have published videos in the year 2022?',
                '9. Which videos have the highest number of comments, and what are their corresponding channel names?'))
        with left_column:
            if st.button("view data"):
                if questions == '1. What are the names of all the videos and their corresponding channels?':
                    with right_column:    
                        st.dataframe(question1())
                
                elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
                    with right_column:
                        st.dataframe(question2())
                elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
                    with right_column:    
                        st.dataframe(question3())
                
                elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
                    with right_column:
                        st.dataframe(question4())
                
                elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
                    with right_column:    
                        st.dataframe(question5())
                
                elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
                    with right_column:
                        st.dataframe(question6())
                        
                elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
                    with right_column:    
                        st.dataframe(question7())
                
                elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
                    with right_column:
                        st.dataframe(question8())
                
                elif questions == '9. Which videos have the highest number of comments, and what are their corresponding channel names?':
                    with right_column:
                        st.dataframe(question9())
                
                
                                
                        
                        
            
            

if __name__ == "__main__":
    main()
