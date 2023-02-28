import errno
import socket
import sys
import select
import time

HOST = ''
SERVER_PORT = 0
LOCAL_IP = '127.0.0.1'
OUTPUT_PORT = 0
LOG_PORT = 0

if(len(sys.argv) < 4):
	print('Please use the correct command: python3 workerpy [workQueueIPAndPort] [outputPort] [syslogPort]\nEnd of processing.')
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

serverSocket =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serverSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

outputSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
logSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#outputSocket.setblocking(0)
#outputSocket.bind(('127.0.0.1', OUTPUT_PORT))

connected = 0

def main():
	global connected
	global serverSocket
	
	connectToServer()

	while True:
		try:
			theMessage = 'ready'
			
			serverSocket.send(theMessage.encode('utf-8'))
			time.sleep(2)
			
			print('no job')
			updateLog('Fetching job\n')
			data = serverSocket.recv(1024)
				
			if(data):
				data = data.decode('utf-8')
				if (not(data.strip() == 'nothing')):
					updateLog('Got job '+data+'\n')
					completedId = doWork(data)
					
					if(completedId > -1):	
						status = (str(completedId) + ' completed').encode('utf-8')
						serverSocket.send(status)
			else:
				connected = 0
				reconnectToServer()
		except KeyboardInterrupt as e:
			print("End of processing.")
			sys.exit(0)
		except Exception as e:
			print(e)
			serverSocket.close()
			sys.exit(0)
		

def connectToServer():	
	global connected
	
	print('connecting to server...')
	while(connected == 0):
		try:  
			serverSocket.connect((HOST, SERVER_PORT))  
			connected = 1 
			print( "connection successful" )
			updateLog("Successfuly connected\n")
		except KeyboardInterrupt as e:
			print("RIP")
			sys.exit(0)
		except ConnectionRefusedError:
			print('connecting to server...')
			time.sleep(1)
			pass
		except Exception as e:  
			print(e)
			print( 'An error occured while trying to connect to the server')
			sys.exit(0)

def reconnectToServer():
	global serverSocket

	serverSocket =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	serverSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	
	updateLog('Trying to reconnect\n')
	connectToServer()

def doWork(theData):
	jobID = -1

	if (len(theData) >= 2):
		data = theData.split(' ', 1)			
		jobID = int(data[0])
		textPart = data[1].split()
		
		print('starting job '+str(jobID))
		for word in textPart:				
			if len(word) > 0:
				print(word)
				try:
					outputSocket.sendto(word.encode('utf-8'),(LOCAL_IP, OUTPUT_PORT))
				except Exception as e:
					print(e)
					pass
					
			time.sleep(0.25)
		updateLog('Done job '+ theData + '\n')
		print('completed job '+str(jobID))

	return jobID

	
def updateLog(theUpdate):		    
	try:
		logSocket.sendto(theUpdate.encode('utf-8'),(LOCAL_IP, LOG_PORT))
	except Exception as e:
		print(e)			
		pass
					
main()

