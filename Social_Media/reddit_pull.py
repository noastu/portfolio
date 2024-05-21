import os
import reddit_backend
import db
import datetime
import pandas as pd
import logging
import sys
import toml

def main(config_path):
    """main function to run Reddit ETL

    Args:
        config_path (str): toml based file with configuration variables
    """
    #read in config va
    with open(config_path, mode="r") as fp:
        config = toml.load(fp)

    #date for logging and query
    today = datetime.datetime.today()

    #set up logging
    download_date = today.strftime('%Y%m%d-%H%M%S')
    log_path = config['paths']['log_path']
    logging.basicConfig(filename=os.path.join(f'{log_path},reddit_pull_{download_date}.log'), 
        datefmt='%Y-%m-%d %H:%M:%S', encoding='utf-8', level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    log = logging.getLogger('apipull')
    
    try:
        log.info('setting up processing variables')
        # set up query date
        query_date = datetime.datetime(today.year, today.month, today.day, 0, 0, 0) - datetime.timedelta(days=0)
        query_date = query_date.strftime('%Y-%m-%dT%H:%M:%SZ')

        #config
        search_obj = reddit_backend.RedditAPI(
            config['redditAPI']['username'],
            config['redditAPI']['password'],
            config['redditAPI']['client'],
            config['redditAPI']['token']
        )
        
        # cities to search
        search_list = config['redditAPI']['search_list']
        
        # loop through list of list and append obj to search_obj list
        for search in search_list: 
            log.info(f'pulling post for {search}')  
            search_obj.pull_posts(search, 5)

        log.info('converting results to dataframe')
        # transform search results into dataframe
        df = search_obj.frame_queries()
        df = df.reset_index(drop=True)

        # send data for file
        data_path = config['paths']['data_path']
        df.to_json(os.path.join(data_path,f'reddit_pull_{download_date}.json'))
        log.info(f"total records pulled: {len(df)}")

        log.info('uploading records to database')
        staging_arg = config['database']['staging_table']
        target_arg= config['database']['target_table']
        schema_arg = config['database']['schema']
        query_arg = config['database']['query']

        db_obj = db.DatabaseHandler(config['database']['connection_string'], df)
        db_obj.merge_data(staging_arg, target_arg, schema_arg, query_arg, True)

        log.info('etl finished')

    except Exception as e:
        log.error(e)
        sys.exit()

if __name__ == "__main__":
    main(sys.argv[1])

        




