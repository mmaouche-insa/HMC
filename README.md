# HMC

Welcome to HMC toolkit. 
A toolkit to test the Location Privacy Protection Mechanism HMC (Heat-Map Confusion).
With this toolkit you can :
 - Launch HMC.
 - Test re-identiication attacks.
 - Evaluate LPPMs with Utility metrics.

This toolkit is a wrap of bigger project [Accio](https://privamov.github.io/accio/)


## Mobility Dataset Format 
One dataset = One directory of mobility traces.
One mobility trace of user =  One CSV file named <user_id>.csv
CSV file format =  Each line is a record of the mobility trace. 
One record =  <lattitude>,<longitude>,<timestamp>
Timestamp = [Unix time POSIX](https://linux.die.net/man/2/time)   .


## Options 
You can see in the table below the list of options.
Only one mode at a time can be used.

| Option 	| Alt                   	| Description                                                    	| type      	| Default 	|
|--------	|-----------------------	|----------------------------------------------------------------	|-----------	|---------	|
| -h     	| --help                	| Print Help                                                     	| /         	| /       	|
| -full  	|                       	| Exeperiment from HMC execution  to Privacy and Utility Metrics 	| /         	| On      	|
| -hmc   	|                       	| Launch only HMC                                                	| /         	| Off     	|
| -atk   	|                       	| Launch Re-identificaiton Attacks                               	| /         	| Off     	|
| -ac    	|                       	| Launch Area Coverage Metric                                    	| /         	| Off     	|
| -sd    	|                       	| Launch Spatial Distortion Metric                               	| /         	| Off     	|
| -o     	| --output              	| Output path                                                    	| Path      	| /       	|
| -d     	| --dataset             	| Dataset Directory path (Full or HMC mode)                      	| Path      	| /       	|
| -kd    	| --known-data          	| Train Dataset Directory path (HMC, ATK, SD or AC mode)         	| Path      	| /       	|
| -ud    	| --unknown-data        	| Test Dataset Directory path (HMC, ATK, SD or AC mode)          	| Path      	| /       	|
| -c     	| --cell-size           	| Heat-Map's cell Size of HMC                                    	| meters    	| 800     	|
| -cac   	| --cell-size-ac        	| Area Coverage's Cell Size                                      	| meters    	| 800     	|
| -cap   	| --cell-size-ap        	| Heat-Map's cell size of AP-Attack                              	| meters    	| 800     	|
| -di    	| --diameter            	| POIs diameter                                                  	| meters    	| 200     	|
| -dt    	| --duration            	| POIs minimal duration                                          	| minutes   	| 60      	|
| -kps   	| --kd-proportion-start 	| Proportion of -d extracted as train (start)                    	| [0.0,1.0] 	| 0.0     	|
| -kpe   	| --kd-proportion-end   	| Proportion of -d extracted as train (end)                      	| [0.0,1.0] 	| 0.5     	|
| -ups   	| --ud-proportion-start 	| Proportion of -d extracted as test (start)                     	| [0.0,1.0] 	| 0.5     	|
| -upe   	| --ud-proportion-end   	| Proportion of -d extracted as test (end)                       	| [0.0,1.0] 	| 1.0     	|


## Run 
```sh
$ java -jar hmc.jar [Options]
```
Simple minimal usage with default value.
```sh
$ java -jar hmc.jar -d <Dataset directory> -o <output>
```
## Contact
https://mmaouche.github.io/

 <mohamed.maouchet@liris.cnrs.fr>
 
License
----

 Copyright LIRIS-CNRS (2017)
 Contributors: Mohamed Maouche  <mohamed.maouchet@liris.cnrs.fr>

This software is a computer program whose purpose is to study location privacy.

This software is governed by the CeCILL-B license under French law and
  abiding by the rules of distribution of free software. You can use,
  modify and/ or redistribute the software under the terms of the CeCILL-B
  license as circulated by CEA, CNRS and INRIA at the following URL
  "http://www.cecill.info".
 
  As a counterpart to the access to the source code and rights to copy,
  modify and redistribute granted by the license, users are provided only
  with a limited warranty and the software's author, the holder of the
  economic rights, and the successive licensors have only limited liability.
 
  In this respect, the user's attention is drawn to the risks associated
  with loading, using, modifying and/or developing or reproducing the
  software by the user in light of its specific status of free software,
  that may mean that it is complicated to manipulate, and that also
  therefore means that it is reserved for developers and experienced
  professionals having in-depth computer knowledge. Users are therefore
  encouraged to load and test the software's suitability as regards their
  requirements in conditions enabling the security of their systems and/or
  data to be ensured and, more generally, to use and operate it in the
 same conditions as regards security.
 
  The fact that you are presently reading this means that you have had
  knowledge of the CeCILL-B license and that you accept its terms.
 



