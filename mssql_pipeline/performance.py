import subprocess
import numpy as np
import time
import ast
import os
import signal
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

class PerformanceTracker:
    """Class to track Memory and CPU utilization during Luigi data pipelines.

    Example:

    pf_tracker = PerfomanceTracker('test','.\\capture_perf.ps1')
    pid = pf_tracker.start()
    --- your run code ---
    pf_tracker.end(pid)
    --- luigi.run() ---
    pf_tracker.store_results('server_test', 'database_test')
    """
    def __init__(self, task_family, ps_script):
        """Instance Attributes

        Args:
            task_family (str): Luigi task family
            ps_script (str): path and file for powershell script
        """
        self.task_family = task_family
        self.ps_script = self._validate_ps_file(ps_script)
        self.workers = []
        self.terminals = []
        self.data = []

    def _validate_ps_file(self, file):
        """Private method to validate if file exists

        Args:
            file (str): path and file for script

        Raises:
            FileNotFoundError: PowerShell script not found

        Returns:
            str: valid file path
        """
        if os.path.exists(file):
            return file
        else:
            raise FileNotFoundError("PowerShell script not found!")

    def start(self, task_id):
        """A method to start subprocess to track cpu and memory

        Args:
            task_id (str): Luigi task_id

        Returns:
            int: pid to keep track of subprocess
        """
        # dictionary to track performance data
        info = {"process_id": None,
                "task_family": self.task_family,
                "task_id": task_id,
                "cpu_avg": None,
                "cpu_max": None,
                "mem_avg": None, 
                "mem_max": None,
                "total_measurements": None,
                "ts": None
              }
        # start tracking
        ps_process = PowershellTracker(self.ps_script)
        ps_process.start()
        # add tracking info and subprocess to collections
        info["process_id"] = ps_process.proc.pid
        self.workers.append(info)
        self.terminals.append({'pid': ps_process.proc.pid, 'subprocess': ps_process})
        return ps_process.proc.pid

    def end(self, worker_id):
        """A method to stop collecting memory and cpu usage results, and kill subprocess 

        Args:
            worker_id (str): process id to identify subprocess
        """
        # filter for correct task
        task_item = next(item for item in self.workers if item["process_id"] == worker_id)
        terminal_item = next(item['subprocess'] for item in self.terminals if item["pid"] == worker_id)
        # collect data
        tracking_stream =  terminal_item.end(worker_id)
        results_formatted = self._format_ps_results(tracking_stream)
        cpu = results_formatted['cpu']
        memory = results_formatted['memory']
        # calculate avg and max
        task_item['cpu_avg'] = 999 if len(cpu) == 0  else "{:.2f}".format(np.average(cpu))
        task_item['cpu_max'] = 999 if len(cpu) == 0 else max(cpu)
        task_item['mem_avg'] = 999 if len(memory) == 0  else "{:.2f}".format(np.average(memory))
        task_item['mem_max'] = 999 if len(memory) == 0 else max(memory)
        task_item['total_measurements'] = len(cpu) + len(memory)
        task_item["ts"] = datetime.today()
        self.data.append(task_item)

    def _format_ps_results(self, results):
        """Private method to format results from powershell

        Args:
            results (io.BufferedReader): stream containing results from subprocess

        Returns:
            list: list of tuples
        """
        result_clean = {}
        memory = []
        cpu = []
        # loop through stream and append valid numerical measurements
        items = results.readlines()
        items_clean = [ast.literal_eval(i.decode('utf-8').strip()) for i in items]
        for measure in items_clean:
            if measure[0] == 999 or measure[1] == 999:
                continue
            else:
                cpu.append(float(measure[0]))
                memory.append(float(measure[1]))
        result_clean['cpu'] = cpu
        result_clean ['memory'] = memory
        return result_clean
    
    def store_results(self, server, database):
        """A Method to store results in SQL Server

        Args:
            server (str): database server
            database (str): database name
        """
        engine = create_engine(
            'mssql+pyodbc://' + server + '/' + database + '?trusted_connection=yes&driver=SQL+Server&TrustServerCertificate=yes&Encrypt=yes',
            fast_executemany=True)
        df = pd.DataFrame(self.data)
        df.to_sql(name='task_performance', con=engine, schema='dbo', method=None, if_exists='append', index=False)

class PowershellTracker:
    """Class to run PowerShell script for tracking performance

    Example:

    ps_tracker = PowershellTracker('.\\capture_perf.ps1')
    id = ps_tracker.start()
    results = ps_tracker.end(id)
    """
    def __init__(self, ps_script):
        """Instance attributes

        Args:
            ps_script (str): path and file for script
        """
        self.id = id
        self.ps_script = ps_script

    def start(self):
        """A method to launch backgroup subprocess to track metrics

        Raises:
            e: Error launching subprocess
        """
        try:
            self.proc = subprocess.Popen(["powershell.exe", 
                                            "-NoProfile", 
                                            "-ExecutionPolicy", 
                                            "Bypass", 
                                            "-File", 
                                            f"{self.ps_script}"],
                                        shell=False, start_new_session=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            raise e


    def end(self, pid):
        """A method to collect subprocess output and terminate subprocess

        Returns:
            io.BufferedReader: Stream of performance measurements
        """
        # rest script for n sec to ensure PowerShell has time to collect data
        time.sleep(5)
        stdout = self.proc.stdout
        os.kill(pid, signal.SIGTERM)
        return stdout