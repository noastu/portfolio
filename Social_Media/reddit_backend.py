import requests
import datetime
import pandas as pd

class RedditAPI:
    """A class to handle Reddit API calls
    """

    def __init__(self, uname, pwd, client, token):
        """Constructor for object
        Args:
            uname (str): username of reddit account
            pwd (str): password of reddit account
            client (str): client id of app
            token (str): secret token for app
        """
        self.uname = uname
        self.pwd = pwd
        self.client = client
        self.token = token
        self._query_basket = []
        self.header = self._generate_headers()

    def _generate_headers(self):
        """A method to generate headers used for authentication

        Returns:
            dict: header dictionary
        """
        # note that CLIENT_ID refers to 'personal use script' and SECRET_TOKEN to 'token'
        auth = requests.auth.HTTPBasicAuth(self.client, self.token)
        # here we pass our login method (password), username, and password
        data = {'grant_type': 'password',
                'username': self.uname,
                'password': self.pwd}
        # setup our header info, which gives reddit a brief description of our app
        headers = {'User-Agent': 'myApp/0.0.1'}
        # send our request for an OAuth token
        res = requests.post('https://www.reddit.com/api/v1/access_token',
                            auth=auth, data=data, headers=headers)
        # convert response to JSON and pull access_token value
        TOKEN = res.json()['access_token']
        # add authorization to our headers dictionary
        headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}
        # while the token is valid (~2 hours) we just add headers=headers to our requests
        requests.get('https://oauth.reddit.com/api/v1/me', headers=headers)
        return headers

    def pull_posts(self, query, n):
        """A method to query reddit by post

        Args:
            query (str): search word
            n (int): number of iterations to search, multiplied by 100
        """
        # initialize dataframe and parameters for pulling data in loop
        df_list = []
        params = {'limit': 100}
        # loop through n times (returning n * 100 posts)
        for i in range(n):
            # make request
            res = requests.get(f"https://oauth.reddit.com/r/{query}/new",
                            headers=self.header,
                            params=params)
            if res.status_code == 200:
                # get dataframe from response
                new_df = self._df_from_response(res)
                # take the final row (oldest entry)
                row = new_df.iloc[len(new_df)-1]
                # create fullname
                fullname = row['kind'] + '_' + row['id']
                # add/update fullname in params
                params['after'] = fullname
                
                # append new_df to data
                df_list.append(new_df)
        
        if len(df_list) > 0:
            # make one big data frame
            df_complete = pd.concat(df_list,ignore_index=True)

            self._query_basket.append(df_complete)
    
    def _df_from_response(self, res):
        """A method to parse query results

        Args:
            res (json): json response from API call

        Returns:
            pandas.DataFrame: Pandas dataframe contains query results
        """
        # initialize temp dataframe for batch of data in response
        df_list = []
        # loop through each post pulled from res and append to df
        for post in res.json()['data']['children']:
            df = pd.DataFrame({
                'subreddit': post['data']['subreddit'],
                'title': post['data']['title'],
                'selftext': post['data']['selftext'],
                'upvote_ratio': post['data']['upvote_ratio'],
                'ups': post['data']['ups'],
                'downs': post['data']['downs'],
                'score': post['data']['score'],
                'link_flair_css_class': post['data']['link_flair_css_class'],
                'created_utc': datetime.datetime.fromtimestamp(post['data']['created_utc']).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'over_18': post['data']['over_18'],
                'id': post['data']['id'],
                'kind': post['kind']
            }, index=[0])
            df_list.append(df)
        data = pd.concat(df_list, ignore_index=True)
        return data

    def frame_queries(self):
        """A method to concatenate queries once all queries are processed

        Returns:
            pandas.DataFrame: Pandas dataframe of all queries
        """
        return pd.concat(self._query_basket)
        


