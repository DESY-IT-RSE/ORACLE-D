# SPDX-License-Identifier: Apache-2.0
# Copyright 2023-2026 Deutsches Elektronen Synchrotron DESY 
#                     and the University of Glasgow
# Authors: Dwayne Spiteri and Gordon Stewart.
# For more information about rights and fair use please refer to src/Main.py.
# For full detailed and legal infomration please read the LICENSE and NOTICE
#    files in the main directory 
# ===========================================================================

import sys
from datetime import datetime, timedelta

from cluster.Cluster import Cluster
from cluster.WorkerNode import *
from datalogger.DataLogger import DataLogger
from jobs.JobScheduler import JobScheduler
from simulation.Time import SimulationTime
from util import Logging


logger = Logging.get_logger()


class Simulation():

    def __init__(self):
        
        self.desiredStartTime             = '2024-01-16 16:00' # STEVE '2018-01-01 00:30' : Starts at the simulation at a set time can be set to any time you wish in the format '2024-01-12 15:00'
        self._simulation_time             = SimulationTime(self.desiredStartTime) #If you want this to be set to the current time, set desiredStartTime to None
        self._simulation_length           = 7*24*3600 # Desired maximum length of the simulation in seconds. (For one year 365*24*3600)
        self._simulation_time._timestep_seconds = 600 # Simulation time step in seconds. #Steve was using 200
        # Finds the half-hour time segment to which the start of the simulation belongs and the one after the end time.
        self._simulation_starting_segment = self._simulation_time.find_hh_segment(self._simulation_time._time)
        self._simulation_maxfinal_segment = self._simulation_time.find_hh_segment(self._simulation_time._time + timedelta(seconds=self._simulation_length), 'next')

        print('Setting up simulation.')
        print('Start date: ' + self._simulation_time._start_time.strftime("%d/%m/%y"))
        print('Timestep: ' + str(self._simulation_time.get_timestep()) + ' seconds')
               
        # Importing average data about the Carbon Intensity of the whole UK grid for the maxumum duration of the simulation.
        # Carbon Intensity data is in gCO2/kWh.
        logger.info('Loading in Carbon Intensity Data')
        datapath = 'data/'
        # datafile = r'gb_carbon_intensity_2023.csv' # or r'gb_carbon_intensity.csv'
        datafile = r'de_carbon_Intensity_2024_15min.csv' # or r'gb_carbon_intensity.csv'
        #Convert start and end segment datetimes to format found in datafile. 
        self.datastart_str = datetime.strftime(self._simulation_starting_segment, '%Y-%m-%dT%H:%M:%S')
        self.datafinal_str = datetime.strftime(self._simulation_maxfinal_segment, '%Y-%m-%dT%H:%M:%S')
        
        linesofimport = []
        datarequired  = False
        with open(datapath+datafile) as file:
            for line in file.read().splitlines():
                line = line.split(',')

                if line[0] == self.datastart_str: # Ignores all lines before the one you want.
                    datarequired = True
                elif line[0] == self.datafinal_str: # Exit file after you have reached the end time value.
                    datarequired = False
                
                if datarequired == True: #Import data when you have found that date you want.
                    if line[1] == '': # If there is data missing
                        print("You are missing forecast CI data for the time segment: " + line[0])
                        sys.exit
                    if line[2] == '': # If there is data missing
                        print("You are missing actual CI data for the time segment: " + line[0])
                        sys.exit
                    linesofimport.append(line)    
                    
        self._CIntendata = linesofimport
        
        self.CIThresholdValue = 400 #  200gCO2e/kWh - This roughly corresponds to what is labelled 'high' in the UK. For Germany I'll put this at 400

        # Create the cluster {WorkerNode[unstantiated WN of type WorkerNode]:amount[integer]}
        # Types of nodes you can currently use are {WorkerNode_h16, WorkerNode_h17, WorkerNode_d20, WorkerNode_d21, WorkerNode_d22, WorkerNode_a23},
        # the details of which can be accessed in node().system, node().cpu, and node().year.
        # You now pass the carbon data you need to estimate the amount of carbon used (manditory)
        # and a flag to set the option of how you will change the operation of the servers during runtime (none, cd1721, cdcd1721 and highforecast) (optional)

        # EXAMPLES
        # Glasgow Uni 
        # self._cluster = Cluster(self._simulation_time, {WorkerNode_h16:13,WorkerNode_h17:43, WorkerNode_d20:40, WorkerNode_d21:32, WorkerNode_d22:36}, self._CIntendata, 'none')
        # self._cluster = Cluster(self._simulation_time, {WorkerNode_d20:40, WorkerNode_d21:32, WorkerNode_d22:36, WorkerNode_d24:17}, self._CIntendata, 'none') # Future 1
        # self._cluster = Cluster(self._simulation_time, {WorkerNode_d20:40, WorkerNode_d21:32, WorkerNode_d22:36, WorkerNode_a24:17}, self._CIntendata, 'none') # Future 2
        # DESY
        self._cluster = Cluster(self._simulation_time, {WorkerNode_DESYT3:40,
                                                        WorkerNode_DESYT4:76, 
                                                        WorkerNode_DESYT11:41, 
                                                        WorkerNode_DESYT13:24, 
                                                        WorkerNode_DESYT16:20, 
                                                        WorkerNode_DESYT17:10, 
                                                        WorkerNode_DESYT26:12, 
                                                        WorkerNode_DESYT31:10, 
                                                        WorkerNode_DESYT382:77}, 
                                                        self._CIntendata, 
                                                        'none', 
                                                        self.CIThresholdValue) # Starting DESY


        print('Cluster: ', end='')
        for node, cores in self._cluster._worker_node_inventory.items():
            n = node(self._simulation_time)
            print(n.hostname + ': ' + str(cores) + ' ', end='')
        print()
        print('Energy saving try: ' + self._cluster._energy_saving_try)
        print('CIThresholdValue: ' + str(self.CIThresholdValue))
        
        # Class to record statistics
        self._datalogger = DataLogger()
        self._cluster.set_datalogger_handlers(self._datalogger.job_submit, 
                                              self._datalogger.job_start, 
                                              self._datalogger.job_finish,
                                              self._datalogger.energy_and_carbon_consumed, 
                                              self._datalogger.peaktime_energy_and_carbon_consumed,
                                              self._datalogger.sum_occupancy )

        # Create a job scheduler to initally seed the cluster with jobs and provide jobs on a regular notice. Needs to know about this cluster
        # Format for initial jobs is a dictionary of {'VO1':jobs, 'VO2':jobs, [...]}
        # Format for regular jobs is a list  of lists of a dictionary of [[{'VO1':jobs per X seconds, 'VO2':jobs per X seconds, [...]}, X], [....] ]
        # self._jobScheduler = JobScheduler(self._simulation_time, self._cluster, {'GridPP':10} , None)
        self._jobScheduler = JobScheduler(self._simulation_time, self._cluster, {'ATLAS':40000,'LHCb':10000} , None)
        # self._jobScheduler = JobScheduler(self._simulation_time, self._cluster, {'GridPP':100000}, [[{'GridPP':250}, 3600*10]])
        self._jobdescript  = "RF20PMTest-50000LHCJobs-Base" # Add here what kind of jobs you are running.
        self._jobdescript  = "RF20PMTest-50000LHCJobs-Base" # Add here what kind of jobs you are running.


        print ('Jobs: ', end='')
        for vo, jobs in self._jobScheduler._inital_job_mix.items():
            print(vo + ': ' + str(jobs), end='')
        if self._jobScheduler._regular_incoming_jobs:
            print(' then ', end='')
            for l1 in self._jobScheduler._regular_incoming_jobs:
                vos = l1[0]
                secs = l1[1]
                for vo, jobs in vos.items():
                    print(vo + ': ' + str(jobs), end=' ')
                print(' per ' + str(secs/3600) +  ' hours', end='')
        print ()
        
        logger.info('Created simulation')
        print(f'Simulation Started. Good Luck')


    def start(self):
        #Permantly run nodes clocked down.
        if self._cluster._energy_saving_try == 'cd':
            for worker_node in self._cluster._worker_nodes:
                worker_node.clock_down()    
        if self._cluster._energy_saving_try == 'cdcd':  
            for worker_node in self._cluster._worker_nodes:
                worker_node.clock_down()
                worker_node.clock_down()

        while True:
            simtottime  = self._simulation_time.get_current_datetime() - self._simulation_time.get_start_datetime() # Simulated Time
            
            # Update the state of the scheduler
            self._jobScheduler.update()

            # Update the state of the cluster
            self._cluster.update() 
            
            # First end condition: When we have no jobs running and no more jobs to submit. Flag will be activate in the cluster update.
            if self._cluster._mission_accomplished == True: 
                realtottime = datetime.now() - self._simulation_time.get_origin_datetime() # Real Time
                self._datalogger.print_summary(True, self._jobdescript, simtottime.total_seconds(), self._simulation_time.get_timestep(), realtottime.total_seconds())
                
                logger.info(f'No more jobs!')
                logger.info(f'Ending simulation at {self._simulation_time.get_current_datetime()}')
                print(f'Simulation Finished. Check logs directory for output')
                sys.exit(0)
            
            # Second end condition: When two weeks in simulation time has passed.
            if simtottime.total_seconds() >= self._simulation_length:
                realtottime = datetime.now() - self._simulation_time.get_origin_datetime() # Real Time
                self._datalogger.print_summary(True, self._jobdescript, simtottime.total_seconds(), self._simulation_time.get_timestep(), realtottime.total_seconds())
                
                logger.info(f'You have been running for a week! Time to stop')
                logger.info(f'Ending simulation at {self._simulation_time.get_current_datetime()}')
                print(f'Simulation Finished. Check logs directory for output')
                sys.exit(0)    

            # Move forward in time
            self._simulation_time.advance() 
            
