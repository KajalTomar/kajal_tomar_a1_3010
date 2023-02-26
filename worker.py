import socket
import sys
import select
import time

HOST = 'grouse.cs.umanitoba.ca'
SERVER_PORT = 8666
LOCAL_IP = '127.0.0.1'
OUTPUT_PORT = 3666
LOG_PORT = 3667

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
			print("RIP")
			serverSocket.close()
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
		except ConnectionRefusedError:
			print('connecting to server...')
			time.sleep(1)
			pass
		except Exception as e:  
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
		textPart = data[1]

		for c in textPart:				
			if c != ' ':
				print(c)
				try:
					outputSocket.sendto(c.encode('utf-8'),(LOCAL_IP, OUTPUT_PORT))
				except Exception as e:
					print(e)
					pass
					
			time.sleep(0.25)
		updateLog('Done job '+ theData + '\n')

	return jobID

	
def updateLog(theUpdate):		    
	try:
		logSocket.sendto(theUpdate.encode('utf-8'),(LOCAL_IP, LOG_PORT))
	except Exception as e:
		print(e)			
		pass
					
main()

