''' 
@file: run_retrieval.py
@author: John3Kim
@desc: Run a retrieval script for Reddit data
'''

from CollectDataFactory import CollectDataFactory
import datetime
import time
from typing import List
from export_to_json import build_json_file
import json
import logging


logging.basicConfig(level=logging.DEBUG,
                    format=' %(asctime)s - %(levelname)s - %(message)s')

def make_date_range(start_date:datetime.time, end_date:datetime.time) -> List[str]: 
    '''
    Creates a list of date ranges

    Arguments:
        start_date: datetime.time -> The start date of the output list
        end_date: datetime.time -> The end date of the output list
    
    Returns: 
        List[str] -> A list of strings containing UNIX datetimes from the start_date to the end_date
    '''

    time_delta = end_date - start_date 
    time_stamps = []

    for num_days in range(time_delta.days + 1):
        day = start_date + datetime.timedelta(days=num_days)
        unix_timestamp = str(int(time.mktime(day.timetuple())))
        time_stamps.append(unix_timestamp)

    return time_stamps

def run_reddit_psio_query(query:str, subreddit:str, start_date:datetime.date, end_date:datetime.date) -> None: 
    ''' 
    Runs a single query for Reddit using pushshift.io as a data source instead of the official 
    Reddit API.

    Arguments:
        query: str -> The search query that you want to run. You can write a single term 
                      or combine with the following boolean commands: 
                      AND, OR, (), NOT
                      
                      Search terms may be stemmed. e.g. dogs resolves as dog for search.

        subreddit:str -> The subreddit, or categories devoted to one topic, that you want to search into.
        start_date: datetime.time -> The start date of the query
        end_date: datetime.time -> The end date of the query
    
    Returns: 
        None -> No object returned but we export queries into three separate files
    '''
    
    reddit_historical = CollectDataFactory()
    reddit_historical = reddit_historical.get_data_source('reddit-historical')
    curr_datetime = datetime.datetime.now().strftime("%Y-%m-%d")

    date_range = make_date_range(start_date, end_date) 
    size_date_range = len(date_range)

    for idx in range(size_date_range-1): 
        start_date = date_range[idx]
        end_date = date_range[idx+1] 

        submissions = reddit_historical.get_submissions(query,subreddit,start_date,end_date)

        # Export or transfer to DB
        build_json_file(submissions["data"],f"submissions_{curr_datetime}")

        # Turn subs into list of strings
        subs_id = [submission_id["id"] for submission_id in submissions["data"]]

        # Get the comments id from the submissions ids
        for sub_id in subs_id:
            subs_id_to_comm_id = reddit_historical.get_comments_id_from_submissions_id(sub_id)
            # Export or transfer to DB
            build_json_file(subs_id_to_comm_id,f"submission_id_to_comment_id_{curr_datetime}")

            list_comments_ids = [comment["comment_id"] for comment in subs_id_to_comm_id]
            
            # If we have a single element with N/A, we have no comments and move on to the next id
            if list_comments_ids[0] != "N/A":
               comments = reddit_historical.get_comments(list_comments_ids)
               build_json_file(comments["data"], f"comments_{curr_datetime}")
            

if __name__ == "__main__": 
    queries=['covid', 'coronavirus','sars-cov-2']
    subreddits=['Canada', 'CanadaPolitics', 'CanadaCoronavirus', 
                'Vancouver', 'Edmonton', 'Winnipeg', 
               'Montreal', 'Ottawa', 'Saskatoon', 
                'Calgary', 'Toronto', 'Ontario', "onguardforthee"]

    start_date = datetime.date(2020,1,1)
    end_date = datetime.date(2020,3,30)
    
    for query in queries: 

        logging.info(f"Running query: {query}")

        for subreddit in subreddits:

            logging.info(f"Running in subreddit: {subreddit}")

            run_reddit_psio_query(query, subreddit, start_date, end_date)