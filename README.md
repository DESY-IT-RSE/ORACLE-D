# The ORACLE-D Framework

## Description
The Optimised Resource Analysis and Carbon Legacy Estimator for Data centres (ORACLE-D) Framework is a framework for simulating different types of compute nodes, seeing how they deal with incoming jobs, and how much power consumed/carbon emitted in doing so. The initial idea was to use this to investigate how energy consumption and/or carbon usage can be reduced an average Grid computing site. This software was written in Python3

## Project status
Version 0.1.0 has been presented at the 2024 HEPiX Spring Workshop in Paris.   
Version 1.0.0: Antares has been tagged for release on 31st March 2026.

| Latest Release | DOI link |
| :------------: | :------: |
| Antares ||



## Current Functionality
The simulation framework is designed to simulate the amount of energy and carbon used* when a computing site[1] performing work[2] is run in different ways[3]. The simulation is modular so [1],[2] and[3] are easily editable. 

\* All the nodes that make up the computing site output the amount of energy they have used every time-step (10 minutes), this is also multiplied by the carbon intensity of the UK grid to estimate the carbon emissions per time-step.

[1] A computing site is made up solely of a specified type(s) and number(s) of compute nodes defined in src/cluster/WorkerNode.py which run work.

[2] The work that the nodes run is made up jobs that are specified in src/jobs/VOJobFactory.py, and are inserted into the simulation either at the beginning of the simulation or at fixed durations throughout the simulation in src/jobs/JobScheduler.py.

[3] The different ways that the simulation can be run is a setting in src/simulation/Simulation.py that changes the frequency the nodes are run at and at what times of day this is done. Current running options are

| Running Flag  | Description |
| :------------: | :------ |
| None          |  Run the nodes as standard  |
| cd            |  Runs all the nodes clocked down one frequency step from the reported maximum frequency for the entire duration of the simulation    | 
| cdcd          |  Runs all the nodes clocked down two frequency steps from the reported maximum frequency for the entire duration of the simulation      |
| cd1721        |  Runs all the nodes clocked down one frequency step from the reported maximum frequency only between the hours of 5pm and 9pm  |
| cdcd1721      |  Runs all the nodes clocked down two frequency steps from the reported maximum frequency only between the hours of 5pm and 9pm   |
| highforecast  |  Runs all the nodes clocked down one frequency step from the reported maximum frequency only when the forecasted usage is high (> 400gCO2e/kWh)   |

The Simulation has two encoded end conditions
  1) All the jobs sent to the cluster have been completed
  2) The amount of time in seconds specified with self._simulation_length has passed

**Outputs**
-  Number of jobs started and finished. 
-  Total and Peak-time (17h-21h) Estimated Energy used in kWh.
-  Total and Peak-time Estimated Carbon (C02e) used in kg.
-  Total and average CPU duration.
-  Total real-time and simulated-time passed.
-  Average occupancy of the cluster

### Package Dependencies
ORACLE-D has external package requirements in requirements.txt

For those that use pip and venv to manage environment, you can run these commands that creates a virtual environment called 'venv' to hold the environment needed for the project. 
```
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

## Getting started with the first run
To run this simulation in this folder type the command:
```
python3 src/Main.py
```

The default running mode is to run 40,000 'ATLAS' and 10,000 'LHCb' jobs on the on the DESY Grid compute cluster from 2024-01-16 16:00 without any special running conditions. This could run for a couple of minutes and produce a log output, and the file RF20PMTest-50000LHCJobs-Base.txt with the summary of the output. The information of grid carbon intensity is taken from data/de_carbon_Intensity_2024_15min.csv. This can be compared to the file that exists already in the folder which takes the same job mix started at the same time but clocks down the machines when the carbon intensity of the grid is forecast in the next time-step to be high. On line 63 of src/simulation/simulation.py, you can see 'high' for the German grid is taken to be 400 gCO2e/kWh and 200 gCO2e/kWh if you want to run on the UK electrical grid.

## Making Small edits
If you want to edit the parameters of the simulation, you will need to change them from src/simulation/Simulation.py. The following are parameters that can be changed.

| Variables to edit  | Description |
| :------------: | :------ |
| desiredStartTime          | The time at which the simulation starts. Leaving this black defaults to clock time  |
| self._simulation_length   | The maximum duration that the will simulation will run for     | 
| self._CIntendata          | The carbon intensity data that is fed into the simulation via the data/ subdirectory |
| self._cluster             | Defines the type and number of worker nodes that form the cluster, the carbon intensity data to use, and flag of how you want to run the cluster during the simulation |
| self._jobScheduler        |  Defines the type and number of jobs to be run on the cluster. Can add jobs into the queue at the start of the simulation, and at regular intervals during the simulation |
| self._jobdescript         | The name of the output text file for the simulation   |

## Adding Extra Options
If you want amend the measurements for each node or add different types of node not yet in the simulation. This needs to be done at the bottom of src/cluster/WorkerNode.py.

To add new machines you will need the following information:
- name
- number of (hyper)threads
- amount of memory available to the node
- the value of the power displaced when node is IDLE
- the value of the power displaced when the node is fully occupied with work at its maximum frequency value
- HEPScore value for the node at its maximum frequency value
- (optional) the value of the power displaced of a fully occupied node at its alternative frequency values
- (optional) the HEPScore value of a fully occupied node at its alternative frequency values

## Copyright and License
Copyright 2023-2026 Deutsches Elektronen Synchrotron DESY and the University of Glasgow

Original Authors: Dwayne Spiteri and Gordon Stewart.

All code in the src/ directory and subsequent subdirectory structure is 
licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Contributors
Dwayne Spiteri and Gordon Stewart


## Acknowledgements
The measurements used here to catagorise the different types of server come from running the [HEPScore23 benchmark](https://w3.hepix.org/benchmarking/how_to_run_HS23.html) on compute nodes.  For the server examples used in ORACLE-D these were taken by **Emanuele Simili** at the University of Glasgow in February 2024 and **Jan Hartmann** at DESY in May of 2025.

The carbon intensity data for the UK is taken from the [UK National Grid ESO](https://www.nationalgrideso.com/data-portal/national-carbon-intensity-forecast/national_carbon_intensity_forecast) interpolated to fill in gaps in the data and can be downloaded from [here](https://www.nationalgrideso.com/data-portal/national-carbon-intensity-forecast/national_carbon_intensity_forecast) and for Germany is taken from [Agorameter](https://www.agora-energiewende.de/daten-tools/agorameter) and [Green Grid Compass](https://www.greengrid-compass.eu/).

This code was partially written for the RF2.0 project that has received funding from the European Union’s Horizon Europe research and innovation programme under grant agreement No. 101131850 and from the Swiss State Secretariat for Education Research and Innovation (SERI)

