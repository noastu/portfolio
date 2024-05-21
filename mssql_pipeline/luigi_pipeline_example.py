import datetime
import os
import logging
import luigi
import tomli
import Example
from DADPy.etl import db, performance
from sys import exit

# runtime variables
with open('.\\config_example.toml', 'rb') as f:
    config = tomli.load(f)
log_path = config['paths']['log_path']
export_path = config['paths']['export_path']
server = config['database']['server']
database = config['database']['database']
ps_script = config['paths']['ps_script']
database_luigi = config['database']['database_luigi']
process_date = datetime.datetime.today().strftime('%Y%m%d-%H%M%S')

# create log object
log_file = os.path.join(log_path, f'Example_{process_date}.log')
logging.basicConfig(filename=log_file, filemode='a', level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger()

# set up processing variables  
Example_obj = Example.SourceData(export_path=export_path)
destination_database = db.DADDatabase(server=server, database=database, log=log, schema='Example')

# track performance
pf = performance.PerformanceTracker('Example', ps_script)

class ExampleDownloadFile(luigi.Task):
    task_namespace = 'Example'
    task_complete = False
    def run(self):
        pid = pf.start(self.task_id)
        log.info('downloading source file from website')
        Example_obj.pull_data()
        pf.end(pid)
        self.task_complete = True

    def complete(self):
        return self.task_complete

class ExampleUploadData(luigi.Task):
    task_namespace = 'Example'
    task_complete = False
    def run(self):
        pid = pf.start(self.task_id)
        log.info('uploading data')
        files = ['Example1.csv', 'Example2.csv']
        for f in files:
            table = f'{f}'.replace('.csv','')
            csv_adpt = db.ImportDbaCsvAdapter(log=log,
                                   server=server,
                                   database=database,
                                   source=os.path.join(export_path, f),
                                   target=table,
                                   delimiter=",",
                                   truncate=True,
                                   schema='Example')
            csv_adpt.upload_file()
        pf.end(pid)
        self.task_complete = True

    def complete(self):
        return self.task_complete
    
    def requires(self):
        return ExampleDownloadFile()

if __name__ == '__main__':
    success = luigi.run()
    if success == False:
        exit(1)
    pf.store_results(server, database_luigi)
    for subproc in pf.terminals:
        os.system("taskkill  /F /pid "+str(subproc['pid']))
    exit(0)
        