Instructions to run assignment 4 files 
** These instructions are for computers in the aviary lab **

# How to run the the message queue file (myWorkQueue.py)
1. Start in the assignment directory. 
2. Run this command in the terminal: 
	`python3 myWorkQueue.py  [clientPort] [workerPort]`
	For example: `python3 myWorkQueue.py 8001 8002`
3. Press **ctrl + z** to stop running this file.  

# How to run the worker file (worker.py)
1. Start in the assignment directory. 
2. Run this command in the terminal: 
	`python3 worker.py [workQueueIPAndPort] [outputPort] [syslogPort]`
	For example: `python3 worker.py grouse.cs.umanitoba.ca:8002 8003 8004`
3. Press **ctrl + z** to stop running this file.  

# How to run the client
1. Run this command in the terminal: 
	`telnet [workQueueIP] [workQueuePort]`
	For example: `telnet grouse.cs.umanitoba.ca 8001`

## Client Commands 

- To run Job (this will return the *job id*)
`job [some text]` 
For example: `job Lorem ipsum dolor sit amet, consectetur adipiscing elit.`

- To check job status
`status [jobid]`
For example: `job 0`

## How to exit telnet sessin
1. type **ctrl + ]**. This changes the command prompt to show as `telnet>`
2. Run `close`
