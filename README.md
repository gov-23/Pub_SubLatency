# Managing the Tradeoff between End-to-End Latency and Load Distribution in Distributed Content-based Filtering Systems
## About
This is the code used to run the simulation for my bachelor's thesis, which was about regarding latency during load distribution phases in a Big Active Data Pub/Sub system. It simulates a distributed content-distribution system with many customizable options
## Usage 
To run the simulation, simply download the files and run the BCS.java file. You have the option to change broker location, subscriber load and location, Initial Deployment heuristic, Dynamcic Migration heuristic and Shuffle algorithm
The master branch currently simulates 2400 different configurations at once, if you want to just test 100, switch to branch "Configurable", where you will find the files.
## Log Files
Log file path needs to be configured before runtime, otherwise you will get an error when trying to execute the file. 