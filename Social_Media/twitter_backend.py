import requests
import datetime
import pandas as pd

class TwitterAPI:
    """A class to handle Twitter API
    """
    # token needs to be set before object is initialized
    token = ''
    # template dictionary for queries
    _param_dict = {'query':'#city',
        'start_time':'query_date',
        'max_results':'100',
        'tweet.fields':'id,created_at,possibly_sensitive'
        }
    # query limit
    MAX_COUNT = 500

    def __init__(self, query_string, query_date):
        """Constructor for object

        Args:
            query_string (string): word to search
            query_date (str): date to search, format is %Y-%m-%dT%H:%M:%SZ'

        Raises:
            ValueError: Bearer Token not set
        """
        if not TwitterAPI.token.split():
          raise ValueError("Bearer Token not set")

        self._query_string = query_string
        self._query_date = query_date
        self._record_count = 0
        self._tweet_basket = []
        self._parameters = self._set_parameters()

    @property
    def query_string(self):
        return self._query_string

    @query_string.setter 
    def query_string(self, val):
        self._query_string = val
        self._parameters = self._set_parameters()

    @property
    def query_date(self):
        return self._query_date

    @query_date.setter 
    def query_date(self, val):
        self._query_date = val
        self._parameters = self._set_parameters()
  
    def _set_parameters(self):
        """A method to format queries

        Returns:
            dict: Formatted query for API call
        """
        parameter_template = self.__class__._param_dict.copy()
        parameter_template.update({'query':self.query_string,'start_time':self.query_date})
        return parameter_template

    def search_tweets(self, parameters=None):  
        """A method to search tweets, parameters are based on class attribute
            until first iteration is passed
        """ 
        # check if parameters are set
        if parameters == None:
            parameters = self._parameters
            
        # ulr and app token
        url = "https://api.twitter.com/2/tweets/search/recent?"
        headers = {
        'Authorization': f'Bearer {self.__class__.token}'
        }
        # search tweet
        response = requests.request("GET", url, headers=headers, params=parameters)
        if response.status_code == 200:
            # parse tweets
            data = response.json()
            self._record_count += int(data['meta']['result_count'])
            for tweet in data['data']:
                id = tweet['id']
                created_at = tweet['created_at']
                text = tweet['text']
                possibly_sensitive = tweet['possibly_sensitive']
                search_string = self.query_string
                # collect tweets for further processing
                self._tweet_basket.append([id, created_at, text, possibly_sensitive, search_string])

            # invoke method again if record count is less than query limit
            if self._record_count < self.__class__.MAX_COUNT:
                try:
                    # find next token and search again
                    next_token = data['meta']['next_token']
                    parameters['next_token'] = next_token
                    self.search_tweets(parameters=parameters)
                except:
                    # reset count
                    self._record_count = 0
                    return None
            else:
                # reset count
                self._record_count = 0
                return None

    def frame_tweets(self):
        """A method to collect tweets once all tweets are collected

        Returns:
            pandas.DataFrame: Pandas Dataframe of tweets
        """
        df = pd.DataFrame(data=self._tweet_basket, columns=['id', 'created_at', 'text', 'possibly_sensitive', 'search_string'])
        return df


