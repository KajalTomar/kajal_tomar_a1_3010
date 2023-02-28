#!/usr/bin/python3

import errno
import socket
import sys
import select
from queue import Queue, Empty


HOST = ''                 
print(socket.gethostname())
CLIENT_PORT = 0            
WORKER_PORT = 0
print("listening on interface " + HOST)


def bindToRandomPort(s):
	result = -1

	while result != 0:
		try:
			result = s.bind((HOST,0))
			return s.getsockname()[1]
		except socket.error as e:
			pass

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

serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serverSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
	serverSocket.bind((HOST, CLIENT_PORT))
except socket.error as e:
	CLIENT_PORT = bindToRandomPort(serverSocket) 
	print('The requested port was in already use. Using available port '+str(CLIENT_PORT)+ ' for the clients.')
serverSocket.setblocking(0);
serverSocket.listen()

workerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
workerSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
	workerSocket.bind((HOST, WORKER_PORT))
except socket.error as e:
	WORKER_PORT = bindToRandomPort(workerSocket)
	print('The requested port was in already use. Using port '+str(WORKER_PORT)+ ' for the workers.')
workerSocket.setblocking(0);    
workerSocket.listen()

print('---\n')

inputs = [ serverSocket , workerSocket ] # not transient
outputs = []

jobsToAdd = []

myClients = []
myWorkers = []

myJobs = [] # will be an array [jobID, jobmessage, jobstatus]
totalJobs = 0

def main():
	global i
	while True:
		try:
			readable, writeable, exceptions = select.select(
				inputs + myClients + myWorkers,
				outputs,
				inputs,
				5
				)
			
			for s in readable:
				if s is serverSocket:
					dealWithServerSocket(s) # new client
				if s is workerSocket:
					dealWithWorkerSocket(s) # new worker
				elif s in myClients:
					dealWithClients(s)
				elif s in myWorkers:
					dealWithWorker(s)
			
		except socket.timeout as e:
			pass
		except KeyboardInterrupt:
			print("End of Process.\n")
			serverSocket.close();
			workerSocket.close();
			sys.exit(0)
		except Exception as e:
			print("There was an error.\nEnd of Process\n")
			print(e)
			print(traceback.format_exc())
			serverSocket.close();
			workerSocket.close();
			sys.exit(0)

def dealWithWorkerSocket(s):
	connection, workerAddress = workerSocket.accept()	
	print('Adding worker ', workerAddress)
	connection.setblocking(0)
	myWorkers.append(connection)
	# give the connection a queue for the data we want to send

def dealWithWorker(s):
	try:
		data = s.recv(1024)
		if data:
			try:
				data = data.decode('utf-8', 'ignore')
				data = data.strip()
			
				# 'done' or 'ready'
				if data == 'ready':
					#print('message: '+data+' >> from %s' % (s.getpeername(),))
					try:
						if(len(jobsToAdd) > 0):
							nextJob = (jobsToAdd.pop(0)).encode('utf-8','ignore')
							nextJobId = int((nextJob.split())[0])
							myJobs[nextJobId][2] = 'IN PROGRESS'
							print('> GET\n<\n')
						else:	
							nextJob = ('nothing').encode('utf-8', 'ignore')
						s.send(nextJob)
					except Exception as e:
						#print('Something went wrong while sending worker %s a job' % (s.getpeername(),))
						#print(e)
						pass
				elif len(data) >= 2:
					#print('message recieved: '+data)
					splitData = data.split()
					#print('jobid = '+ splitData[0])
					jobId = int(splitData[0])
					jobStatus = splitData[1].lower()
					#print('status: '+jobStatus)
					if(jobStatus == 'completed' and jobId < len(myJobs) and jobId >= 0):
						myJobs[jobId][2] = 'COMPLETED'
						print('> DONE ' + str(jobId) +'\n<\n')
			except UnicodeDecodeError:
				print('removing worker' % (s.getpeername(),))
				s.close()
				if s in myWorkers:
					myWorkers.remove(s)
		else:
			print('removing worker')
			s.close()
			if s in myWorkers:
				myWorkers.remove(s)
	except ConnectionResetError:
		print('removing worker')
		s.close()
		if s in myWorkers:
			myWorkers.remove(s)

def dealWithServerSocket(s):
	# new client
	connection, clientAddress = serverSocket.accept()
	print('Adding client ', clientAddress)
	connection.setblocking(0)
	myClients.append(connection)

def dealWithClients(s):
	global jobsToAdd

	# read
	try:
		data = s.recv(1024)
		if data:
			try:
				data = data.decode('utf-8','ignore')
				commandTypeResult, resultID = resolveClientCommand(data) # was a new job command	
				if(commandTypeResult == 0 and resultID >= 0):
					print('> JOB '+myJobs[resultID][1]+'\n<')
					s.send((str(resultID)+'\n').encode('utf-8','ignore'))
				
					# add the job to the queue
					thisJob = str(resultID) + ' ' + myJobs[resultID][1]
					jobsToAdd.append(thisJob)
				elif(commandTypeResult == 1 and resultID >= 0 and resultID < len(myJobs)):
					toSend = 'job '+ str(resultID) + ' is in state '+myJobs[resultID][2]+'\n'
					print('> STATUS ' + str(resultID) +'\n<')
					s.send((toSend).encode('utf-8','ignore'))
			except UnicodeDecodeError:
				print('Removing client' % (s.getpeername(),))
				s.close()
				if s in myClients:
					myClients.remove(s)
		else:
			print('Removing client')
			s.close()
			myClients.remove(s)
	except ConnectionResetError:
		print('Removing client')
		s.close()
		if s in myClients:
			myClients.remove(s)

def resolveClientCommand(theCommand):
	global totalJobs
	command = ""
	returnID = -1
	commandType = -1 # not valid command

	if(len(theCommand) > 0):
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

main()
	

