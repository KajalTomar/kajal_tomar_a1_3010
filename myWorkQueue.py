# --------------------------------------------------------------------
# NAME		        : Kajal Tomar
# STUDENT NUMBER	: 7793306
# COURSE		    : COMP 3010
# INSTRUCTOR	    : Robert Guderian
# ASSIGNMENT	    : Assignment 1
#
# REMARKS: 	This file implements the job queueue 
#			that accepts new jobs and issues the jobs to workers
# --------------------------------------------------------------------

#!/usr/bin/python3
import errno
import socket
import sys
import select

HOST = ''                 
CLIENT_PORT = 0            
WORKER_PORT = 0

# --------------------------------------------------------
# bindToRandomPort(socket)
#
# Purpose: binds the socket to an available port
# Parameter: socket to bind
# Returns: the port number that the socket is bound to
# --------------------------------------------------------
def bindToRandomPort(s):
	result = -1

	while result != 0:
		try:
			result = s.bind((HOST,0))
			return s.getsockname()[1]
		except socket.error as e:
			pass

# Confirm that arguments are valid. If not print instructions for acceptable arguments then end the process.
if(len(sys.argv) < 3):
	print('Please use the correct command: python3 myQueue.py  [clientPort] [workerPort]\nEnd of processing.')
	sys.exit(0)
else:
	clientPortArg = int(sys.argv[1].strip())
	workerPortArg= int(sys.argv[2].strip())
	if((1024 <= clientPortArg <= 65535) and (1024 <= workerPortArg <= 65535) and workerPortArg != clientPortArg):
		CLIENT_PORT = int(clientPortArg)
		WORKER_PORT = int (workerPortArg)
	else:
		print(sys.argv)
		print('Try again, port numbers must be different and must be in range [1024 - 65535].\nEnd of processing.')
		sys.exit(0)


# Create, bind and listen to 2 TCP sockets (one for clients, one for workers)

# set up the socket for the clients
clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
clientSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
	clientSocket.bind((HOST, CLIENT_PORT))
except socket.error as e:
	CLIENT_PORT = bindToRandomPort(clientSocket)  
	print('The requested port was in already use. Using available port '+str(CLIENT_PORT)+ ' for the clients.')
clientSocket.setblocking(0);
clientSocket.listen()

# set up the socket for the workers
workerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
workerSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
	workerSocket.bind((HOST, WORKER_PORT))
except socket.error as e:
	WORKER_PORT = bindToRandomPort(workerSocket)
	print('The requested port was in already use. Using port '+str(WORKER_PORT)+ ' for the workers.')
workerSocket.setblocking(0);    
workerSocket.listen()

# the variables to hold socket and job info
inputs = [ clientSocket , workerSocket ] 
outputs = []

jobsToAdd = [] # will act as a queue for jobs that need to be processed

myClients = [] 
myWorkers = []

myJobs = [] # will hold the state of all the jobs [jobID, jobmessage, jobstatus]
totalJobs = 0

# print meta data before beginning 
print('-------------------------------------------')
print('> [clientPort] [workerPort]')
print('> ['+str(CLIENT_PORT)+'] ['+str(WORKER_PORT)+']')
print('> listening on interface: '+ socket.gethostname())
print('-------------------------------------------\n')


# --------------------------------------------------------
# runJobQueue
#
# Purpose: 	Uses `select` to monitor sockets. Call approporiate
#			function that corresponds with the socket.
# --------------------------------------------------------
def runJobQueue():
	while True:
		try:
			# uses `select`, track all the TCP clients in respective lists
			readable, writeable, exceptions = select.select(
				inputs + myClients + myWorkers,
				outputs,
				inputs,
				5
				)
			
			for s in readable:
				if s is clientSocket:
					dealWithClientSocket(s) # new client
				if s is workerSocket:
					dealWithWorkerSocket(s) # new worker
				elif s in myClients:
					dealWithClients(s) # client sent message
				elif s in myWorkers:
					dealWithWorker(s) # worker sent message
			
		except socket.timeout as e:
			pass
		except KeyboardInterrupt: 
			print("End of Process.\n")
			clientSocket.close();
			workerSocket.close();
			sys.exit(0)
		except Exception as e:
			print("There was an error with myWorkQueue.\nEnd of Process\n")
			#print(e)
			#print(traceback.format_exc())
			clientSocket.close();
			workerSocket.close();
			sys.exit(0)


# --------------------------------------------------------
# dealWithWorkerSocket(socket)
#
# Purpose: 	Accepts the socket's connection and adds it to 
#			the list of workers
# Parameter: socket that is trying to connect
# --------------------------------------------------------
def dealWithWorkerSocket(s):
	connection, workerAddress = workerSocket.accept()	
	print('Adding worker ', workerAddress)
	connection.setblocking(0)
	myWorkers.append(connection)

# --------------------------------------------------------
# dealWithWorker(socket)
#
# Purpose: 	Recieves data from a worker socket. If the 
#			worker is ready for a job, then assign it one
#			from the job queue.
#			If the worker is sended a 'completed job' status,
#			update our list of all jobs with the new state.
# Parameter: socket that has sent a message
# --------------------------------------------------------
def dealWithWorker(s):
	try:
		data = s.recv(1024)
		if data:
			try:
				data = data.decode('utf-8', 'ignore')
				data = data.strip()
			
				# valid message will be 'completed jobID' or 'ready' 
				if data == 'ready':
					# worker is ready for a job
					try:
						if(len(jobsToAdd) > 0): # if there is a job
							nextJob = (jobsToAdd.pop(0)).encode('utf-8','ignore') # pop from the queue so that each jobs is issued to exactly 1 worker
							nextJobId = int((nextJob.split())[0])
							myJobs[nextJobId][2] = 'IN PROGRESS'
							print('> GET\n<\n')
						else:	# otherwise send 'nothing'
							nextJob = ('nothing').encode('utf-8', 'ignore')
						s.send(nextJob)
					except Exception as e:
						pass
				elif len(data) >= 2:
					splitData = data.split()
					jobId = int(splitData[0])
					jobStatus = splitData[1].lower()
					# the worker completed a job, update that job's status to 'done'
					if(jobStatus == 'completed' and jobId < len(myJobs) and jobId >= 0): 
						myJobs[jobId][2] = 'COMPLETED'
						print('> DONE ' + str(jobId) +'\n<\n')
			except UnicodeDecodeError:
				# assume we should remove the worker if the data sent is bad
				print('removing worker' % (s.getpeername(),))
				s.close()
				if s in myWorkers:
					myWorkers.remove(s)
		else: # remove the worker if they didn't send any data
			print('removing worker')
			s.close()
			if s in myWorkers:
				myWorkers.remove(s)
	except ConnectionResetError:
		print('removing worker')
		s.close()
		if s in myWorkers:
			myWorkers.remove(s)

# --------------------------------------------------------
# dealWithClientSocket(socket)
#
# Purpose: 	Accepts the socket's connection and adds it to 
#			the list of clients
# Parameter: socket that is trying to connect
# --------------------------------------------------------
def dealWithClientSocket(s):
	# new client
	connection, clientAddress = clientSocket.accept()
	print('Adding client ', clientAddress)
	connection.setblocking(0)
	myClients.append(connection)

# --------------------------------------------------------
# dealWithClient(socket)
#
# Purpose: 	Read's the socket's message to see if it's adding
#			a job or asking for the status. If it's a job
#			add it to the job queue.
# Parameter: socket that sent the message
# --------------------------------------------------------
def dealWithClients(s):
	global jobsToAdd

	# read
	try:
		data = s.recv(1024)
		if data:
			try:
				data = data.decode('utf-8','ignore')
				commandTypeResult, resultID = resolveClientCommand(data) # check what the command was
				
				if(commandTypeResult == 0 and resultID >= 0): # handle JOB command 
					print('> JOB '+myJobs[resultID][1]+'\n<')
					s.send((str(resultID)+'\n').encode('utf-8','ignore'))
				
					# add the job to the queue
					thisJob = str(resultID) + ' ' + myJobs[resultID][1]
					jobsToAdd.append(thisJob)

				elif(commandTypeResult == 1): # handle STATUS command

					if (resultID >= 0 and resultID < len(myJobs)): # if jobID is valid, send it's status
						toSend = 'job '+ str(resultID) + ' is in state '+myJobs[resultID][2]+'\n'
						print('> STATUS ' + str(resultID) +'\n<')

					else: # if the jobID is not valid, send a message notifying the client
						toSend = 'job '+str(resultID)+' does not exist\n'
					s.send((toSend).encode('utf-8','ignore'))
			except UnicodeDecodeError:
				# remove the client if they send bad message
				print('Removing client' % (s.getpeername(),))
				s.close()
				if s in myClients:
					myClients.remove(s)
		else:
			# remove the client if they didn't send a message
			print('Removing client')
			s.close()
			myClients.remove(s)
	except ConnectionResetError:
		print('Removing client')
		s.close()
		if s in myClients:
			myClients.remove(s)

# --------------------------------------------------------
# resolveClientCommand(the command)
#
# Purpose: 	parses the data to see if it was a job or a 
#			status command. If its a job then add it to the
# 			list of all jobs.
# Parameter: socket that is trying to connect
# Returns:	(-1,-1) if command was invalid
#			(0, jobID) if the command was 'JOB'  
#			(1, jobID) if the command was 'STATUS'
# --------------------------------------------------------
def resolveClientCommand(theCommand):
	global totalJobs
	command = ""
	returnID = -1 # default
	commandType = -1 # not valid command

	if(len(theCommand) > 1):
		theData = theCommand.split(' ', 1)
		command = str(theData[0])  

		if(command.lower() == 'job' and len(theData) >= 2):
			# create job array and add it to the array of jobs 
			# return the jobID
			theData = str(theData[1])
			theData = str(theData.splitlines()[0])
			
			commandType = 0
			returnID = totalJobs
			myJobs.append([returnID, theData,'WAITING'])
			
			#print(myJobs)
			totalJobs+=1
		elif(command.lower() == 'status' and len(theData) >= 2):
			commandType = 1
			try:
				returnID = int(theData[1])
			except ValueError:
				returnID = -1

	return (commandType, returnID)

runJobQueue() # to run the job queue
	

