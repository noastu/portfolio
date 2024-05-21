import datetime
import pandas as pd
import subprocess
import base64
from shapely import wkt
import geopandas as gpd
from sqlalchemy import create_engine, inspect

class DatabaseMergeError(Exception):
    pass

class ImportDBACsvError(Exception):
    pass

class MyDatabase:
    """Internal Class to handle My Database
    """
    def __init__(self, server, database, log, schema='dbo'):
        """Constructor for class

        Args:
            server (str): server name
            database (str): database name
            log (logging): ETL log
        """
        self.server = server
        self.database = database
        self.log = log
        self.schema = schema
        self.engine = self.__build_engine()

    def __build_engine(self):
        """Create engine based on server and database

        Returns:
            sqlalchemy.create_engine: database engine
        """
        engine = create_engine(
            'mssql+pyodbc://' + self.server + '/' + self.database + '?trusted_connection=yes&driver=SQL+Server&TrustServerCertificate=yes&Encrypt=yes',
            fast_executemany=True)
        return engine
    
    def select_data(self, query):
        """select query on database

        Args:
            query (str): select query for database

        Returns:
            pandas.DataFrame: dataframe containing query
        """
        df = pd.read_sql(query, self.engine)
        return df
    
    def truncate_table(self, table):
        """Delete records from table. Named truncate becuase all records are deleted
        Args:
            table (str): name of table
        """
        if inspect(self.engine).has_table(table, schema=self.schema):
            query = f'delete from [{self.schema}].[{table}];'
            with self.engine.begin() as con:
                con.execute(query)
            self.log.info('Truncated Table ' + table)
        else:
            self.log.info(f"{table} does not exist, skipping truncate")

    def update_database(self, table, file, capture_time=(False, None), file_kwargs={'sep':',',
                                                                            'engine':'python',
                                                                             'dtype':str,
                                                                             'chunksize':10000}):
        """update database from csv file

        Args:
            table (str): name of table
            file (str): name of csv file
            capture_time (tuple), optional): generate timestamp. Defaults to (False, None).
            file_kwargs (dict, optional): parameters to interact with read_csv. Defaults to {'sep':',', 'engine':'python', 'dtype':str, 'chunksize':10000}.
        """
        # Define variables for monitoring batches
        batch = 0
        rows = 0
        total = 0
        # Set up a loop of reading chunk and writing chunk to SQL Server. Ensure you set the right encoding for the file you are transferring in the parameters.
        if capture_time[0]:
            batch_date = datetime.datetime.now()
            if capture_time[1] is not None:
                batch_date_col = capture_time[1]
            else:
                batch_date_col = 'batch_date'
        for chunk in pd.read_csv(file, **file_kwargs):
            if capture_time[0]:
                chunk[batch_date_col] = batch_date
            chunk.to_sql(name=table, con=self.engine, schema=self.schema, method=None, if_exists='append', index=False)
            # Print information about each batch that was written.
            batch = batch + 1
            rows = len(chunk.index)
            total = total + rows
            self.log.info('Table: ' + table + ' Batch: ' + str(batch) + ' Rows: ' + str(rows) + ' Overall Rows: ' + str(total))

    def merge_data(self, table, file, file_kwargs={'sep':',',
                                                                            'engine':'python',
                                                                             'dtype':str,
                                                                             'chunksize':10000}):
        """Merge data from z_dynamic_staging table to target table. Ensure 
        columns are labeled COL1, COL2, COL3 ... and z_merge_columns table has 
        metadata for sproc.

        Args:
            table (str): name of table
            file (str): name of csv file
            capture_time (tuple), optional): generate timestamp. Defaults to (False, None).
            file_kwargs (dict, optional): parameters to interact with read_csv. Defaults to {'sep':',', 'engine':'python', 'dtype':str, 'chunksize':10000}.
        Raises:
            DatabaseMergeError: Staging table has records and must be cleared first
        """
        df = self.select_data('select COL1 from z_staging_dynamic')
        if df['COL1'].count() > 0:
            raise DatabaseMergeError(f"Table contains {str(df['COL1'].count())} records, clear table before merging data")
        capture_time=(False, None)
        self.update_database('z_staging_dynamic', file, capture_time, file_kwargs)
        self.log.info(f"merging data to {table}")
        with self.engine.begin() as con:
                con.execute(f"usp_dynamic_merge {table}")
    
    @staticmethod
    def format_merge_columns(df):
        """Utility method to change DataFrame columns to COL1, COL2, COL3....
        for merge process. 

        Args:
            df (pandas.DataFrame): DataFrame to process

        Returns:
            pandas.DataFrame: DataFrame with changed columns
        """
        # loop through df columns and change to COL(i)
        formatted_cols = []
        cols = df.columns.tolist()
        for i in range(len(cols)):
            formatted_cols.append(f'COL{str(i+1)}')
        df.columns = formatted_cols
        return df
    
    def schema_check(self, sources, metadata_cols=None, file_kwargs={'sep':'|',
                                                 'engine':'python',
                                                 'dtype': str,
                                                 'nrows': 2}):
        """Check if any files have new columns that are not in database table.

        Args:
            sources (dict): Dictionary containing table_name and file_name. Example {'my_table': 'C:\\Users\\MyMember\\test.csv'}
            file_kwargs (dict, optional): parameters to interact with read_csv. Defaults to {'sep':'|', 'engine':'python', 'dtype':str, 'nrows': 2}.

        Raises:
            ValueError: Data Source has new columns
        """
        
        schema_check = []
        self.log.info("checking schema changes")
        for key, value in sources.items():
            result = self._compare_sources(value, key, metadata_cols, file_kwargs)
            schema_check.append(result)
        if sum(schema_check) > 0:
            raise ValueError('Data Source has new columns!')
        else:
            return None
    
    def _compare_sources(self, file, table, metadata_cols, file_kwargs):
        """Compare file to table

        Args:
            file (str): file to read
            table (str): database table
            file_kwargs (dict): parameters to interact with read_csv.

        Returns:
            int: 0 if new columns are not found else 1 
        """
        df = pd.read_csv(file, **file_kwargs)
        if metadata_cols:
            for item in metadata_cols:
                df[item] = None
        file_columns = df.columns.values.tolist()
        insp = inspect(self.engine)
        columns_table = insp.get_columns(table, self.schema)
        db_columns = [c['name'] for c in columns_table]
        file_columns.sort()
        db_columns.sort()
        if file_columns == db_columns:
            self.log.info(f'{file}: no changes')
            return 0
        else:
            self.log.info(f'{file} has new columns: {[source for source in file_columns if source not in db_columns]}')
            return 1

class GeoMyDatabase(MyDatabase):
    """Extended MyDatabase class to interact with spatial data
    """
    def __init__(self, server, database, log):
        super().__init__(server, database, log)
    def select_data(self, query, column):
        """select data from database with spatial column

        Args:
            query (str): Spatial SQL query with WKT formatted column
            column (str): WKT column of spatial data

        Returns:
            geopandas.GeoDataFrame: GeoDataFrame of select query with WKT as geometry
        """
        df = super().select_data(query)
        df['geometry'] = df[column].apply(wkt.loads)
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
        gdf.drop(columns=[column], inplace=True, axis=1)
        return gdf

class ImportDbaCsvAdapter:
    """A class to import flat files to My Database based on PowerShell Import-DbaCsv 
    dbatools: https://docs.dbatools.io/Import-DbaCsv 
    """
    def __init__(self, log, server, database, source, target, delimiter, truncate=False, schema='dbo'):
        """Constructor for class

        Args:
            log (logging): ETL log
            server (str): server name
            database (str): database name
            source (str): path and file of database
            target (str): database table name
            delimiter (str): file delimiter (tab, pipe, semicolon, space, and comma)
            truncate (bool, optional): Bool to truncate table. Defaults to False.
        """
        self.log = log
        self.server = server
        self.database = database
        self.source = source
        self.target = target
        self.delimiter = delimiter
        self.truncate = truncate
        self.schema = schema
    
    def upload_file(self):
        """Run PowerShell script to import files
        """
        # prepare statement to run truncate
        if self.truncate:
            command = f'Import-DbaCsv -Path {self.source} -SqlInstance {self.server} -Database {self.database} -Table {self.target} -Schema {self.schema} -Truncate -Delimiter "{self.delimiter}" -AutoCreateTable -EnableException'
           
        # run without truncate
        else:
            command = f'Import-DbaCsv -Path {self.source} -SqlInstance {self.server} -Database {self.database} -Table {self.target} -Schema {self.schema} -Delimiter "{self.delimiter}" -AutoCreateTable -EnableException'
        # Encode the command in UTF-16 LE, then Base-64 for PowerShell
        commandBase64 = base64.b64encode(command.encode("utf-16-le")).decode()
        # Run the encoded PowerShell command and capture its output
        proc = subprocess.run( f"powershell.exe -EncodedCommand {commandBase64}", 
                            capture_output=True, shell=True, encoding="utf-8")
        if proc.returncode != 0:
            raise ImportDBACsvError(proc.stderr)
        else:
            self.log.info(proc.stdout)
            
            
