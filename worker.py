import socket
import sys
import select
import time

HOST = 'grouse.cs.umanitoba.ca'
PORT = 8666

serverSocket =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serverSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#serverSocket.settimeout(5)
socks = [ serverSocket]

connected = 0

def main():
	global connected
	connectToServer()

	while connected == 1:
		try:
			theMessage = 'ready'
			for s in socks:
				s.send(theMessage.encode('utf-8'))
				time.sleep(2)
			
			for s in socks:
				data = s.recv(1024)
				
				if(data):
					data = data.decode('utf-8')
					print(data)					
				else:
					connected = 0
					s.close()
#		except socket.timeout as e:
#			print('timeout')
#			connectToServer()
#			pass
		except KeyboardInterrupt as e:
			print("RIP")
#			serverSocket.close()
			sys.exit(0)
		except Exception as e:
#			serverSocket.close()
			print(e)

def connectToServer():	
	global connected
 	# attempt to reconnect, otherwise sleep for 2 seconds  
	print('connecting to srver')
	while(connected == 0):
		for s in socks:
			try:  
				s.connect((HOST, PORT))  
				connected = 1 
				print( "connected" )  
			except socket.error:  
				time.sleep( 2 )
main()

