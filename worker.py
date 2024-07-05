# --------------------------------------------------------------------
# REMARKS: 	This file implements the worker. Workers fetch jobs from 
#			the work queue and “do the work”. 
# --------------------------------------------------------------------

import errno
import socket
import sys
import select
import time

# variables to hold host, port numbers
HOST = ''
SERVER_PORT = 0
LOCAL_IP = '127.0.0.1'
OUTPUT_PORT = 0
LOG_PORT = 0

# Confirm that arguments are valid. If not print instructions for acceptable arguments then end the process.
if(len(sys.argv) < 4):
	print('Please use the correct command: python3 worker.py [workQueueIPAndPort] [outputPort] [syslogPort]\nEnd of processing.')
	sys.exit(0)
else:
	result = (sys.argv[1]).split(':')
	if(len(result) != 2):
			print(sys.argv[1]+' is invalid. Please enter the [workQueueIPAndPort]')
			print('For example: grouse.cs.umanitoba.ca:8001')
			sys.exit(0)
	else:
		serverHost = result[0]
		serverPortArg =	int(result[1])
		outputPortArg = int(sys.argv[2])
		logPortArg= int(sys.argv[3])
		
		# make sure the requested ports are all within the valid range and different from one another
		if((len(serverHost) > 0) and (1024 <= serverPortArg <= 65535) and (1024 <= outputPortArg <= 65535) and (1024 <= logPortArg <= 65535) and (serverPortArg != outputPortArg) and (serverPortArg != logPortArg) and (outputPortArg != logPortArg)):
			HOST = serverHost
			SERVER_PORT = serverPortArg
			OUTPUT_PORT = outputPortArg
			LOG_PORT = logPortArg
			print(sys.argv)
		else:
	  		print(sys.argv)
	  		print('Try again, port numbers must be different and must be in range [1024 - 65535].\nEnd of processing.')
	  		sys.exit(0)

# create the socket for the server (job queue)
serverSocket =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serverSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# create the socket for the syslog and output
outputSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
logSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

connected = 0 # not connected to the server initially

# --------------------------------------------------------
# main
#
# Purpose: 	Connect / reconnect to server.
# 			Poll to fetch jobs and send message to notify
#			server when the job is completed.
# --------------------------------------------------------
def main():
	global connected
	global serverSocket
	
	connectToServer()

	while True:
		try:
			# poll to fetch jobs
			theMessage = 'ready'
			serverSocket.send(theMessage.encode('utf-8'))
			time.sleep(2)
			print('no job')
			updateLog('Fetching job\n')
			
			# check if we got 'nothing' or a job
			data = serverSocket.recv(1024)
				
			if(data):
				data = data.decode('utf-8')
				if (not(data.strip() == 'nothing')):
					updateLog('Got job '+data+'\n') 
					completedId = doWork(data) # got job, do work
					
					if(completedId > -1):	
						# send completed status for the job
						status = (str(completedId) + ' completed').encode('utf-8')
						serverSocket.send(status)
			else:
				# assumer no data means we have disconnected from the server, try to reconnect
				connected = 0
				reconnectToServer()
		except KeyboardInterrupt as e:
			print("End of processing.")
			sys.exit(0)
		except Exception as e:
			print("End of processing.")
			serverSocket.close()
			sys.exit(0)
		
# --------------------------------------------------------
# connectToServer()
#
# Purpose: 	Keeps trying to connect to the server until it's 
#			succcesful or something bad happens.
# --------------------------------------------------------
def connectToServer():	
	global connected
	
	print('connecting to server...')
	
	while connected == 0:
		try:  
			serverSocket.connect((HOST, SERVER_PORT))  
			connected = 1 
			print( "connection successful" )
			updateLog("Successfuly connected\n")
		except KeyboardInterrupt as e:
			print("End of processing.\n")
			sys.exit(0)
		except ConnectionRefusedError:
			print('connecting to server...')
			time.sleep(1)
			pass
		except Exception as e:  
			print('An error occured while trying to connect to the server\n')
			print('End of processing.\n')
			sys.exit(0)

# --------------------------------------------------------
# reconnectToServer()
#
# Purpose:	create new socket, and try to connect to the 
#			server (job queue) again
# --------------------------------------------------------
def reconnectToServer():
	global serverSocket

	serverSocket =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	serverSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	
	updateLog('Trying to reconnect\n')
	connectToServer()

# --------------------------------------------------------
# doWork(data)
#
# Purpose:	'do the work' so print out each word one at a 
#			time every 0.25 seconds. 
#			Push each word to the output (OUTPUT_PORT) 
#			and push the completed status
#			to the syslog (LOG_port) once the job is done .
#			These are both UDP messages.
# Parameter: the string that has the job id and text to print
# Returns: the jobID of the completed job
# --------------------------------------------------------
def doWork(theData):
	jobID = -1

	if (len(theData) >= 2):
		data = theData.split(' ', 1) # extract the job id and the text
		jobID = int(data[0])
		textPart = data[1].split()
		
		print('starting job '+str(jobID))
		
		for word in textPart:	# print each word in the text			
			if len(word) > 0:
				print(word)
				try:
					outputSocket.sendto(word.encode('utf-8'),(LOCAL_IP, OUTPUT_PORT)) # udp message to the output
				except KeyboardInterrupt as e:
					print("End of processing.\n")
					sys.exit(0)
				except Exception as e:
					#print(e)
					pass
					
			time.sleep(0.25)
		updateLog('Done job '+ theData + '\n') # udp message to the syslog
		print('completed job '+str(jobID))

	return jobID


# --------------------------------------------------------
# (data)
#
# Purpose:	pushes the update to the sysLog (uses UDP)
# Parameter: the string that is the update
# --------------------------------------------------------
def updateLog(theUpdate):		    
	try:
		logSocket.sendto(theUpdate.encode('utf-8'),(LOCAL_IP, LOG_PORT))
	except KeyboardInterrupt as e:
			print("End of processing.\n")
			sys.exit(0)
	except Exception as e:
		# print(e)			
		pass
					
main()

