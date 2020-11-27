''' 
@file: retrieve_social_media_data.py
@author: John3Kim
@desc: Make an OOP-based implementation to collect social media 
from different sources such as Reddit and Twitter.

TODO: Simplify the get_submissions and get_comments methods
'''

from abc import ABC, abstractmethod
from typing import List, Dict
import datetime
from datetime import date, timedelta
import time
import json
import requests
import logging 
from export_to_json import build_error_text_file


logging.basicConfig(level=logging.DEBUG,
                    format=' %(asctime)s - %(levelname)s - %(message)s')

class CollectRedditDataAbstract(ABC):
    ''' 
    Abstract class for Reddit data to show what methods we need to incorporate
    '''

    def __init__(self) -> None: 
        pass 

    @abstractmethod
    def get_submissions() -> None:
        raise NotImplementedError("Please implement the get_submissions method.")

    @abstractmethod
    def get_comments_id_from_submissions_id(self) -> None:
        raise NotImplementedError("Please implement the get_comments_id_from_submissions_id method.") 

    @abstractmethod
    def get_comments(self) -> None: 
        raise NotImplementedError("Please implement the get_comments method.") 


class CollectRedditPSIOData(CollectRedditDataAbstract): 
    '''
    Methods needed to collect Reddit data 

    We need two sub-classes: 
    one for pushshift.io -> Historical data
    one for praw -> recent/real-time data
    This class involves pushshift.io only

    Attributes __init__ : 

        self.time_delay: int -> Time delay that is used for each RESTful API request (in seconds). Default is 2 seconds.
        self.number_results: int -> The number of results that can be retrieved from each request.  Ranges from 25 to 500. 
                                    Default is 500 but a request may not retrieve 500 results.
    '''
    def __init__(self) -> None: 
        self.time_delay = 2 
        self.number_results = 500
    
    def get_submissions(self, query:str, subreddit:str, before_date:str, after_date:str, num_retries:int = 1) -> json: 
        '''
        Get a set of reddit submissions. This represents a single call of the posts endpoint. 

        Arguments: 
            query: str -> The search query that you want to run. You can write a single term 
                or combine with the following boolean commands: 
                AND, OR, (), NOT
                
                Search terms may be stemmed. e.g. dogs resolves as dog for search.

            subreddit:str -> The subreddit, or categories devoted to one topic, that you want to search into.
            start_date: datetime.time -> The start date of the query.
            end_date: datetime.time -> The end date of the query.
            num_retries:int -> In the event that a request fails, we rerun the submissions query.  Default is 1 retry.
        
        Returns: 
            json: A JSON object that contains the results of a single request using the submissions endpoint.
        '''
        for retry in range(num_retries + 1):

            try:
                submissions_api_call = f'https://api.pushshift.io/reddit/search/submission/?q={query}&subreddit={subreddit}&after={before_date}&before={after_date}&limit={self.number_results}'
                submissions_requests = requests.get(submissions_api_call)
                time.sleep(self.time_delay)

            except json.decoder.JSONDecodeError:
                logging.info(f"Failed to parse: {submissions_api_call}")
                logging.info(f"Export error file.") 
                build_error_text_file(submissions_api_call,"submissions_error")
                logging.info(f"Retrying request.")
                continue

            except requests.HTTPError as http_error: 
                logging.info(f"HTTP error: {http_error}") 
                logging.info(f"Retrying request.")
                continue
            
            # If we're successful in obtaining our data, no need to retry
            break

        return submissions_requests.json()

    def get_comments_id_from_submissions_id(self, submission_id:str, num_retries:int = 1) -> List[Dict[str,str]]: 
        '''
        Given a single submissions id (called id), represented as base36, we obtain the comments id.

        This is dependent on obtaining ids from the posts

        Arguments: 
            submission_id:str -> The submisson id that is retrieved from pusushift.io's submissions endpoint.
            num_retries:int -> In the event that a request fails, we rerun the submissions query.  Default is 1 retry.
        
        Returns: 
            List[Dict[str,str]] -> A list of dictionaries where each element of the list will contain the original submission 
                                   id and its corresponding comments id.  Multiple elements means that there are multiple comments 
                                   per submission id.

        '''

        list_comments_submission_id = []

        for retry in range(num_retries + 1):

            try:
                submission_id_api_call = f"https://api.pushshift.io/reddit/submission/comment_ids/{submission_id}"
                sub_id_to_comment_id_requests = requests.get(submission_id_api_call)
                time.sleep(self.time_delay)

                sub_id_to_comment_id_requests = sub_id_to_comment_id_requests.json()["data"]
                # turn the list of ids into a json into an object

                for comment_id in sub_id_to_comment_id_requests: 
                    list_comments_submission_id.append({"submission_id":submission_id,"comment_id": comment_id})

            except json.decoder.JSONDecodeError:
                logging.info(f"Failed to parse: {submission_id_api_call}")
                logging.info(f"Export error file.") 
                build_error_text_file(submission_id_api_call,"submission_id_error")
                logging.info(f"Retrying request.")
                continue

            except requests.HTTPError as http_error: 
                logging.info(f"HTTP error: {http_error}") 
                logging.info(f"Retrying request.")
                continue
            
            # If we're successful in obtaining our data, no need to retry
            break
        
        if len(list_comments_submission_id) == 0: 
            return [{"submission_id":submission_id, "comment_id": "N/A"}]

        return list_comments_submission_id

    def get_comments(self, list_comments_ids: List[str], num_retries:int = 1) -> json: 

        '''
        Gets the comments of a post, based on the comments ids.

        Arguments: 
            list_comments_ids: List[str] -> Each element will contain a comments id, represented as a base-36 string.  
            num_retries:int -> In the event that a request fails, we rerun the submissions query.  Default is 1 retry.
        '''
        
        strings_comments = ','.join(list_comments_ids)

        for retry in range(num_retries + 1):

            try:
                comments_api_call = f'https://api.pushshift.io/reddit/search/comment?ids={strings_comments}'
                comments_requests = requests.get(comments_api_call)
                time.sleep(self.time_delay)

            except json.decoder.JSONDecodeError:
                logging.info(f"Failed to parse: {comments_api_call}")
                logging.info(f"Export error file.") 
                build_error_text_file(comments_api_call,"comment_error")
                logging.info(f"Retrying request.")
                continue

            except requests.HTTPError as http_error: 
                logging.info(f"HTTP error: {http_error}") 
                logging.info(f"Retrying request.")
                continue
            
            # If we're successful in obtaining our data, no need to retry
            break

        return comments_requests.json()