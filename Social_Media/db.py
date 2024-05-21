from sqlalchemy import create_engine, inspect
import pandas as pd
import datetime

class DatabaseHandler:
    """A Class to handle database management during ETL
    """

    def __init__(self, conn_string, data = pd.DataFrame):
        """Constructor for object

        Args:
            conn_string (str): connection string for database
            data (pd.DataFrame, optional): Dataframe to send to database. Defaults to pd.DataFrame.
        """
        # create engine
        self.engine = create_engine(conn_string)
        self.data = data

    def append_data(self, target, schema, batch = True):
        """A method to append data to database. Creates batch date by default.

        Args:
            target (str): target table in database
            schema (str): database schema
            batch (bool, optional): flag to create batch data. Defaults to True.

        """
        # create batch date if true
        if self.batch:
            self.data['batch_date'] = datetime.datetime.now()
        else:
            pass
        # append data to table    
        self.data.to_sql(target, schema=schema, con=self.engine, if_exists='append', index=False)

    def merge_data(self, source, target, schema, query, batch = True):
        """A method to run SQL statement to merge data between staging and destination tables.
            Creates batch date by default.

        Args:
            source (str): staging table
            target (str): destination table
            schema (str): database schema
            query (dict): a dictionary to read sql based on type
            batch (bool, optional): flag to create batch data. Defaults to True.
        """
        # get query
        raw_query = self.format_query(query)
        # create timestamp
        if batch:
            self.data['batch_date'] = datetime.datetime.now()
        else:
            pass
        # empty table if exists, just in case
        if inspect(self.engine).has_table(source):
            with self.engine.connect() as con:
                rs = con.execute(f"truncate table {source}")
        # load source data
        self.data.to_sql(source, con=self.engine, schema=schema, if_exists='append', index=False)
        # create target if not exist
        if inspect(self.engine).has_table(target) == False:
            self.data.to_sql(target, schema=schema, con=self.engine, if_exists='append', index=False)
        else:
            pass
        # run merge query
        with self.engine.connect() as con:
            rs = con.execute(raw_query)

    def format_query(self, query_dict):
        """_summary_

        Args:
            query_dict (dict): dictionary of query type

        Returns:
            str: string of sql
        """
        #check file type
        if query_dict['query_type'] == "file":
            #read query into sting
            with open(query_dict['value'], mode="r") as f:
                raw_query = f.read()
            return raw_query
        else:
            #query already sql. example sql stored procedure
            return query_dict['value']