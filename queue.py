#!/usr/bin/python3

import socket
import sys
import select
import queue

HOST = 'grouse.cs.umanitoba.ca'                 
CLIENT_PORT = 8098              # Arbitrary non-privileged port
WORKER_PORT = 8784
print("listening on interface " + HOST)

serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

serverSocket.bind((HOST, CLIENT_PORT))
serverSocket.setblocking(0);
serverSocket.listen()

workerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
workerSocket.bind((HOST, WORKER_PORT))
workerSocket.setblocking(0);
workerSocket.listen()

inputs = [ serverSocket , workerSocket ] # not transient
outputs = []

# outgoing message queues (socket:Queue)
clientMessageQueues = {}

myClients = []

myJobs = [] # will be an array [jobmessage, jobstatus]
totalJobs = 0

def main():
	
	while True:
		try:
#			print('waiting for input')
			readable, writeable, exceptions = select.select(
				inputs + myClients,
				outputs,
				inputs,
				5
				)
			#		print('released from block')

			for s in readable:
				if s is serverSocket:
					dealWithServerSocket(s)
# new client
#				connection, client_address = serverSocket.accept()
#					print('Adding client ', client_address)
#					connection.setblocking(0)
#					myClients.append(connection)
					# Give the connection a queue for data we want to send
				elif s in myClients:
					# read 
					data = s.recv(1024)
					if data:
						try:
							data = data.decode('utf-8')
							commandTypeResult, resultID = resolveClientCommand(data) # was a new job command	
							if(commandTypeResult == 0 and resultID >= 0):
								print('> JOB '+myJobs[resultID][0]+'\n<')
								s.send((str(resultID)).encode('utf-8'))
							elif(commandTypeResult == 1 and resultID >= 0 and resultID < len(myJobs)):
								toSend = 'job '+ str(resultID) + ' is in state '+myJobs[resultID][1]+'\n'
								print('> STATUS ' + str(resultID) +'\n<')
								s.send((toSend).encode('utf-8'))
						except UnicodeDecodeError:
								s.close()
								myClients.remove(s)
					else:
						myClients.remove(s)
					
				#print('connection from: ')
				#print(client_address)

		except socket.timeout as e:
			#print('timeout')
			pass
		except KeyboardInterrupt:
			print("RIP")
			serverSocket.close();
			sys.exit(0)
		except Exception as e:
			print("Something happened... I guess...")
			print(e)
			sys.exit(0)

def dealWithServerSocket(s):
	# new client
	connection, client_address = serverSocket.accept()
	print('Adding client ', client_address)
	connection.setblocking(0)
	myClients.append(connection)

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
			myJobs.append([theData,'WAITING'])
			#print('JOB '+myJobs[totalJobs][1])
			totalJobs+=1
			
			#returnData = myJobs
			#print('JOB received'+data.decode('UTF-8'))
		elif(command.lower() == 'status' and len(theData) >= 2):
			commandType = 1
			returnID = int(theData[1]) 

	return (commandType, returnID)

main()






