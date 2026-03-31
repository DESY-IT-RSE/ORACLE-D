# SPDX-License-Identifier: Apache-2.0
# Copyright 2023-2026 Deutsches Elektronen Synchrotron DESY 
#                     and the University of Glasgow
# Authors: Dwayne Spiteri and Gordon Stewart.
# For more information about rights and fair use please refer to src/Main.py.
# For full detailed and legal infomration please read the LICENSE and NOTICE
#    files in the main directory 
# ===========================================================================

from util import Logging
logger = Logging.get_logger()



class WorkerNode():

    def __init__(self, simulation_time, hostname='', threads=0, memory=0, idlepower=0, freq_dep_specs={3.0:(300,3000), 2.0:(200,2000), 1.0:(100,1000)}):
        '''Creates a worker node to be simulated.
        The first argument (simulation_time) is the common simulation time.
        The second argument (hostname)       is the name of the worker node.
        The third argument (threads)         is the number of threads the machine has - usually this is equal to or double the number of cores (default is double)
        The fourth argument (memory)         is the about of RAM that is available on the core
        The fifth argument (idlepow)         is the average instantaneous power dissipated by an idle node in W.
        The sixth argument (freq_dep_specs)  is the variable that contains all the frequency-dependednt specifications that are be measured. Starting with the maximum 
                                                as the first element which will be the default. Here it is condensed to a dictionary of the frequencies (keys) in GHz 
                                                a machine can run at, (value) paired with a tuple that corresponds to the average* instantaneous power dissipated in W,
                                                and the the score for the machine when HEPSCORE v1.5 + Ver 2.2 of the HEPScore suite is run at max frequency.   
                                                * = The <75%-95%> - the average of all points between and incuding the 75th and 95th percentiles of power-arranged data
                                                Format: {frequency_i:(power_i,HEPScore_i), frequency_j:(power_j,HEPScore_j), [...]}
        OLD || The seventh argument (duration)      is the time in seconds it takes for a node to run HEPSCORE v1.5 + Ver 2.2 of the HEPScore suite on max frequency.                                                                                             
        '''
        self._simulation_time = simulation_time
        self._hostname = hostname
        self._number_of_threads = threads
        self._number_of_cores = self._number_of_threads/2 # Differentiate for power consumption purposes.
        self._busy_cores = 0
        self._maximum_RAM = memory
        self._busy_RAM = 0
        self._jobs = []

        self._powerusage_idle = idlepower/3600 # Energy use (Needs to convert from hours to seconds)
        # Frequency-dependent specifications.
        self._frequencies = []
        self._powers_available = []
        self._HEPScores = []

        for freqs in sorted(freq_dep_specs.keys(), reverse=True): 
            (power, HEPScore) = freq_dep_specs[freqs]
            self._frequencies.append(freqs)
            self._powers_available.append(power/3600)
            self._HEPScores.append(HEPScore)

        
        self._running_frequency = self._frequencies[0]
        self._previous_frequency = self._frequencies[0]
        
        self._max_HEPScore = self._HEPScores[0]

        self._powerusage_active = self._powers_available[0]   

        # Performance multipliers
        self._fixed_duration_multiplier = (1939.60)/(self._max_HEPScore) # 1 The relative HEPScore on this machine running a standard benchmark compared to 2*AMD Milano (d22) node at max frequency.
        self._dynamic_duration_multiplier = 1 # For use of relative duration scaling when clocking up/down nodes.

        # Node descriptors
        self._system = "Arthur C Clarke"
        self._cpu = "HAL 9000"
        self._year = "3001"

        # Handlers
        self._job_started_handler = None
        self._job_finished_handler = None
    
    # -----------------
    # Getters
    # -----------------
    @property
    def cpu(self):
        return self._cpu  
    
    @property
    def hostname(self):
        return self._hostname
    
    @property
    def jobs(self):
        return self._jobs
    
    @property
    def system(self):
        return self._system
    
    @property
    def year(self):
        return self._year

    @property
    def busy_RAM(self):
        return self._busy_RAM
    @property
    def max_RAM(self):
        return self._maximum_RAM
    
    @property
    def busy_cores(self):
        return self._busy_cores
    @property
    def number_of_cores(self): # People care about the number of places that run jobs so they will interchange cores with threads. 
        return self._number_of_threads

    @property
    def powerusage_active(self):
        return self._powerusage_active
    @property
    def powers_available(self):
        return self._powers_available
    @property
    def powerusage_idle(self):
        return self._powerusage_idle
    
    @property
    def frequencies_available(self):
        return self._frequencies
    @property
    def running_frequency(self):
        return self._running_frequency

    @property
    def HEPScore_vs_frequency(self):
        return self._HEPScores
    @property
    def max_HEPScore(self):
        return self._max_HEPScore

    # -----------------
    # Setters
    # -----------------
    @hostname.setter
    def hostname(self, value):
        if isinstance(value, str) == False: 
            raise TypeError("The type of hostname should be `string` ")
        self._hostname = value

    @powerusage_idle.setter
    def powerusage_idle(self,value):
        if value > self._powerusage_active:
            raise ValueError("An active power consumption less than an idle power is not possible.")  
        self._powerusage_idle = value

    @powerusage_active.setter
    def powerusage_active(self,value):
        if value < self._powerusage_idle:
            raise ValueError("An active power consumption less than an idle power is not possible.")   
        self._powerusage_active = value

    @busy_RAM.setter
    def busy_RAM(self,value):
        if value > self._maximum_RAM:
            raise ValueError("Cannot allocate more RAM than is available on the machine.")  
        self._busy_RAM = value

    @running_frequency.setter
    def running_frequency(self,value):
        if value not in self.frequencies_available:
            raise ValueError("The machine cannot be set to run at this frequency.")  
        self._previous_frequency = self.running_frequency
        self._running_frequency = value   
    # -----------------
    # Functions
    # -----------------
    def set_datalogger_handlers(self, job_started, job_finished):
        self._job_started_handler = job_started
        self._job_finished_handler = job_finished


    def is_awaiting_jobs(self):
        return self.get_free_core_count() > 0

    def get_free_core_count(self):
        return self._number_of_threads - self._busy_cores
    
    def get_memory_available(self):
        return self._maximum_RAM - self._busy_RAM
  
    def can_schedule_job(self, job):
        if job.cores_req > self.get_free_core_count(): return False
        if job.memory_req > self.get_memory_available(): return False
        return True
     
    def start_job(self, job):
        job.start_time = self._simulation_time.get_current_datetime()
        # Adjust sampled duration according to worker node performance
        job.duration *=  self._fixed_duration_multiplier 
        # Adjust end times based on how the nodes are running
        freqindex = self.frequencies_available.index(self._running_frequency)
        currentHEPScore = self._HEPScores[freqindex]
        job.duration *= self.max_HEPScore/currentHEPScore


        self._job_started_handler(job, self)
        self._jobs.append(job)
        self._busy_cores += job.cores_req
        self._busy_RAM   += job.memory_req*job.cores_req
            

    def timestep_power_dissipated(self):
        # Outputs the amount of power used by a machine in a timestep in kWh. 
        # Assume that machines always use the idle power amount but power dissipated scales linearly with number of cores used to the maximum. 
        baseusage = self._powerusage_idle
        maxusage  = self._powerusage_active
        physicalcores = self._number_of_cores
        coresactive = self._busy_cores # Actually the number of threads active in a HT system.
        
        # In a HT system, each core runs two threads and the load is usually balances, so to a good approximation, the max energy output is when
        # half the threads are in use, one running on every core. A core roughly will not consume more power by running 2 threads instead of 1.
        scaling = coresactive/physicalcores 
        if scaling > 1: scaling = 1

        inst_pow_disp = maxusage * scaling   
        if inst_pow_disp < baseusage: inst_pow_disp = baseusage # Can't expend less power than the idle.
        
        inst_pow_disp_timestep = inst_pow_disp * self._simulation_time.get_timestep() # Scale up from power per second to power per timestep.
        inst_pow_disp_timestep = inst_pow_disp_timestep/1000 # Convert from Wh to kWh.

        return inst_pow_disp_timestep
    

    def change_clock_speed(self, clockspeed):
        # Function to change the clock speed or frequency of a node - unit is GHz
        self.running_frequency = clockspeed
        
        for job in self._jobs:
            # Change the amount of time left on all jobs running on the node
            timeLeftOnJob  = job.end_time - self._simulation_time.get_current_datetime()
            newJobDuration = timeLeftOnJob * self._dynamic_duration_multiplier
            job.end_time = self._simulation_time.get_current_datetime() + newJobDuration
            

    def clock_down(self):
        # Function to decrease the frequency of a node by one available frequency-step.
        freqindex = self.frequencies_available.index(self._running_frequency)
        try: 
            # Change the power setting according to the frequency
            self._powerusage_active = self.powers_available[freqindex+1]
            # Alter the length of a job ratio of the HEPScore in the frequency you are moving to w.r.t the one you are moving from
            self._dynamic_duration_multiplier = self._HEPScores[freqindex]/self._HEPScores[freqindex+1]

            self.change_clock_speed(self.frequencies_available[freqindex+1])
        except IndexError:
            print(f"This machine, {self.hostname}, is already running at its lowest frequency: {self.frequencies_available[freqindex]} GHz.")
      

    def clock_up(self):    
        # Function to increase the frequency of a node by one available frequency-step.
        freqindex = self.frequencies_available.index(self._running_frequency)
        if freqindex-1 >= 0:
            # Change the power setting according to the frequency
            self._powerusage_active = self.powers_available[freqindex-1]
            # Change the length of a job by the job-extension-factor in the frequency you are moving from 
            self._dynamic_duration_multiplier = self._HEPScores[freqindex]/self._HEPScores[freqindex-1]

            self.change_clock_speed(self.frequencies_available[freqindex-1])
        else:
            print(f"This machine, {self.hostname}, is already running at its maximum frequency: {self.frequencies_available[freqindex]} GHz.")
  
    #### @lp.profile
    def update(self):
        remaining_jobs = []
        finished_jobs = []

        for job in self._jobs:
            # Has the job finished?
            if job.end_time <= self._simulation_time.get_current_datetime():
                if job not in finished_jobs:
                    job.duration = (job.end_time - job.start_time).seconds # need to update the duration of the job if it has been edited due to clockdowns
                    self._job_finished_handler(job, self)
                    finished_jobs.append(job)
                    self._busy_RAM   -= job.memory_req*job.cores_req # Need to free up the memory now it is no longer being used.
                    self._busy_cores -= job.cores_req  # Need to free up the cores now they are no longer being used.
            else:
                remaining_jobs.append(job)

        self._jobs = remaining_jobs

                                                         
class WorkerNode_h16(WorkerNode):
    def __init__(self, simulation_time, hostname=''):
        super().__init__(simulation_time, "h16"+hostname, 32, 64, 70, {2.4:(210,350), 2.2:(205,290), 2.0:(200,250), 1.8:(188,230), 1.6:(160,199), 1.4:(140,170), 1.2:(120,140)})

        #Node descriptors
        self._system = "HPE ProLiant DL60 Gen9"
        self._cpu    = "Intel Xeon E5-2630 v3"
        self._year   = "2016"
    
class WorkerNode_h17(WorkerNode):
    def __init__(self, simulation_time, hostname=''):
        super().__init__(simulation_time, "h17"+hostname, 40, 160, 70, {2.2:(211,388.77), 2.0:(170,321.26), 1.8:(159,290.37), 1.6:(146,259.06), 1.4:(136,227.16), 1.2:(128,195.06)})
      
        #Node descriptors
        self._system = "HPE ProLiant DL60 Gen9"
        self._cpu    = "Intel Xeon E5-2630 v4"
        self._year   = "2017"

class WorkerNode_d20(WorkerNode):
    def __init__(self, simulation_time, hostname=''):
        super().__init__(simulation_time, "d20"+hostname, 128, 500, 134, {2.35:(447,1839.97), 2.0:(285,1249.84), 1.5:(253,949.23)})
          
        #Node descriptors
        self._system = "Dell PowerEdge C6525 - Milano"
        self._cpu    = "AMD EPYC 7452"
        self._year   = "2020"

class WorkerNode_d21(WorkerNode):
    def __init__(self, simulation_time, hostname=''):
        super().__init__(simulation_time, "d21"+hostname, 128, 500, 150, {2.35:(447,1839.97), 2.0:(285,1249.84), 1.5:(253,949.23)})
        
        #Node descriptors
        self._system = "Dell PowerEdge C6525 - Milano"
        self._cpu    = "AMD EPYC 7452"
        self._year   = "2021"

class WorkerNode_d22(WorkerNode):
    def __init__(self, simulation_time, hostname=''):
        super().__init__(simulation_time, "d22"+hostname, 128, 500, 150, {2.6:(486,1939.60), 2.0:(306,1332.15), 1.5:(255,1010.57)})
        
        #Node descriptors
        self._system = "Dell PowerEdge C6525 - Milano"
        self._cpu    = "AMD EPYC 7513"
        self._year   = "2022"

class WorkerNode_a23(WorkerNode):
    def __init__(self, simulation_time, hostname=''):
        super().__init__(simulation_time, "a23"+hostname, 160, 500, 176, {3.0:(550,2622.49), 2.9:(473,2548.79), 2.8:(465,2489.48), 2.7:(457,2420.19), 2.6:(449,2358.93), 2.5:(441,2289.09), 2.4:(433,2230.03), 2.3:(425,2154.63), 2.2:(415,2088.41), 2.1:(408,2011.27), 2.0:(401,1943.41), 1.0:(308,1086.41)})
        
        self._number_of_cores = self._number_of_threads

        #Node descriptors
        self._system = "Ampere Mt Collins 2U"
        self._cpu    = "Ampere Altra Q80-30"
        self._year   = "2023"

class WorkerNode_x24(WorkerNode):
    def __init__(self, simulation_time, hostname='Grace'):
        super().__init__(simulation_time, hostname+"-x24", 144, 480, 180, {3.4:(845,4313.20), 3.2:(716,4161.82), 3.0:(575,3968.11), 2.8:(497,3746.97), 2.6:(431,3532.47), 2.4:(386,3307.89), 2.2:(341,3050.42), 2.0:(307,2798.62), 1.5:(269,2210.00), 1.0:(233,1570.54), 0.5:(203,819.57)})

        self._number_of_cores = self._number_of_threads

        #Node descriptors
        self._system = "SuperMicro 2U"
        self._cpu    = "Nvidia Grace"
        self._year   = "2024"

class WorkerNode_a24(WorkerNode):
    def __init__(self, simulation_time, hostname=''):
        super().__init__(simulation_time, hostname+"-a24", 128, 512, 200, {3.0:(416, 2178.56)})
        
        self._number_of_cores = self._number_of_threads

        #Node descriptors
        self._system = "XMA"
        self._cpu    = "Ampere Altra Q128-30"
        self._year   = "2024"      

class WorkerNode_d24(WorkerNode):
    def __init__(self, simulation_time, hostname=''):
        super().__init__(simulation_time, hostname+"-d24", 128, 576, 140, {3.1:(324.9, 1838.52), 2.30:(336.5, 1891.50), 1.90:(282.2, 1490.98), 1.50:(248.8, 1196.23) })

        #Node descriptors
        self._system = "SuperMicro"
        self._cpu    = "AMD EPYC 8534P - Sienna"
        self._year   = "2024"   

# Machines used at DESY - GRID
class WorkerNode_DESYT3(WorkerNode):
    def __init__(self, simulation_time, hostname=''):
        super().__init__(simulation_time, hostname+"Type3", 96, 256, 112, {2.7:(530.1, 1450), 2.5:(530.1, 1450), 2.3:(330, 1102), 2.1:(332.4, 1100), 1.9:(333.3, 1102), 1.7:(330.7, 1102)})

        #Node descriptors
        self._system = "DELL PowerEdge R6525"
        self._cpu    = "AMD EPYC 7402 24-Core Processor"
        self._year   = "2020"     

class WorkerNode_DESYT4(WorkerNode):
    def __init__(self, simulation_time, hostname=''):
        super().__init__(simulation_time, hostname+"Type4", 40, 128, 112, {2.4:(224.14, 392.74), 2.2:(212.15, 363.84), 2.0:(201.30, 333.06), 1.8:(190.34, 297.68), 1.6:(179.26, 271.44), 1.4:(169.33, 239.32), 1.2:(160.90,206.97)})        
        
        #Node descriptors
        self._system = "Dell PowerEdge R430"
        self._cpu    = "Intel(R) Xeon(R) CPU E5-2640 v4"
        self._year   = "2017"          
class WorkerNode_DESYT11(WorkerNode):
    def __init__(self, simulation_time, hostname=''):
        super().__init__(simulation_time, hostname+"Type11", 224, 512, 336, {2.0:(739.18, 2587.39),	1.8:(687.38, 2343.30), 1.6:(666.87, 2132.99), 1.4:(598.68, 1833.76), 1.2:(548.73, 1624.60), 1.0:(509.22, 1378.12), 0.8:(490.61, 1115.09)})

        #Node descriptors
        self._system = "Lenovo ThinkSystem SR630 V3"
        self._cpu    = "Intel(R) Xeon(R) Platinum 8480CL"
        self._year   = "2023"

class WorkerNode_DESYT13(WorkerNode):
    def __init__(self, simulation_time, hostname=''):
        super().__init__(simulation_time, hostname+"Type13", 96, 256, 180, {2.85:(566.94, 1617.00), 2.7:(567.3, 1607.11), 2.5:(567.73, 1618.62), 2.3:(569.78, 1599.07), 2.1:(326.06, 993.060), 1.9:(325.39, 1012.81), 1.7:(326.39, 1005.73), 1.5:(277.43, 724.80)})

        #Node descriptors
        self._system = "DELL PowerEdge R6525"
        self._cpu    = "AMD EPYC 7443 24-Core Processor"
        self._year   = "2021"                                                  

class WorkerNode_DESYT16(WorkerNode):
    def __init__(self, simulation_time, hostname=''):
        super().__init__(simulation_time, hostname+"Type16", 96, 256, 124, {2.3:(526, 1061.087), 2.2:(523.82, 1029.79), 2.0:(522.14, 1022.08), 1.8:(378.41, 718.390),1.6:(377.92, 712.100), 1.4:(378.38, 722.230), 1.2:(308.38, 481.970)})

        #Node descriptors
        self._system = "Supermicro BB 1023US-TR4 H11DSU-iN"
        self._cpu    = "AMD EPYC 7451 24-Core Processor"
        self._year   = "2018"          
class WorkerNode_DESYT17(WorkerNode):
    def __init__(self, simulation_time, hostname=''):
        super().__init__(simulation_time, hostname+"Type17", 32, 96, 140, {2.6:(268.36, 319.09), 2.4:(249.67, 296.82), 2.2:(234.68, 274.31), 2.0:(223.23, 250.40), 1.8:(212.02, 227.82), 1.6:(199.62, 204.70), 1.4:(192.03, 179.94), 1.2:(184.37, 155.34)})

        #Node descriptors
        self._system = "Dell PowerEdge R430"
        self._cpu    = "Intel(R) Xeon(R) CPU E5-2640 v3"
        self._year   = "2015"   
class WorkerNode_DESYT26(WorkerNode):
    def __init__(self, simulation_time, hostname=''):
        super().__init__(simulation_time, hostname+"Type26", 96, 256, 187, {3.2:(658, 1704.539), 3.1:(655.80, 1614.25), 2.9:(654.74, 1606.07), 2.7:(656.35, 1604.79), 2.5:(652.88, 1610.74), 2.3:(405.32, 1049.96), 2.1:(402.85, 1044.47), 1.9:(402.91, 1050.89), 1.7:(403.93, 1049.78), 1.5:(333.35, 689.870)})

        #Node descriptors
        self._system = "DELL PowerEdge R6525"
        self._cpu    = "AMD EPYC 74F3 24-Core Processor"
        self._year   = "2022"  

class WorkerNode_DESYT31(WorkerNode): #Estimated HEPScore
    def __init__(self, simulation_time, hostname=''):
        super().__init__(simulation_time, hostname+"Type31", 96, 256, 168, {3.2:(648, 1602.597), 3.1:(637.24, 1536.16), 2.9:(469.84, 1388.77), 2.7:(470.60, 1389.34), 2.5:(368.51, 1163.13)})

        #Node descriptors
        self._system = "DELL PowerEdge R6525"
        self._cpu    = "AMD EPYC 7F72 24-Core Processor"
        self._year   = "2021"  

# OBSELETE NODES
class WorkerNode_DESYT382(WorkerNode): 
    def __init__(self, simulation_time, hostname=''):
        super().__init__(simulation_time, hostname+"Type382", 1, 1, 59, {2.5:(289, 335), 2.4:(225.08, 251.55) , 2.2:(224.75, 250.83) , 2.0:(224.71, 251.84) , 1.8:(226.53, 250.59) , 1.6:(224.79, 250.59) , 1.4:(225.15, 251.69) , 1.2:(225.33, 251.63)})

        #Node descriptors
        self._system = "Dell PowerEdge M520"
        self._cpu    = "Intel(R) Xeon(R) CPU E5-2450 v2"
        self._year   = "2014"