'''
@file: CollectDataFactory 
@name: John3Kim 
@desc: The factory method where we choose what classes to instantiate, instead of 
knowing the class that you want to call into. 
'''
from CollectRedditData import CollectRedditPSIOData


class CollectDataFactory(): 
    ''' 
    The only class that we need to call in order to retrieve data. 
    Initially it'll include Reddit's pushshift.io and reddit's initial libraries 
    but it'll soon be able to incorporate other libraries such as Twitter
    '''

    def __init__(self): 
        pass
    
    def get_data_source(self, data_source:str) -> object: 
        ''' 
        Calls a class that handles the collection of data in a particular data source
        -> Identified social media include Twitter and Reddit (pushshift.io and PRAW)

        Arguments: 
            data_source: str -> The data source that you want to search on. 
                                You can search for the following: 
                                    -> reddit-historical: Collects Reddit data from pushshift.io
        Returns: 
            object -> Returns a class that handles data retrieval.  If the data_source is not 
                      defined, we return a None object.
        '''
        if data_source.lower() == "reddit-historical": 
            return CollectRedditPSIOData()

        return None
