

from AE33_device import AE33_device
#import socket
#import sys
#import socket
#import time
#import os

#import AE33_device

#sock1 = socket.socket()
#sock1.connect(("192.168.1.98", 3000)) 



device = AE33_device()

device.read_path_file()
device.print_params()

device.MAXID = device. MINID
print(device.MAXID)

device.connect()
device.request('HELLO',0,0)
#device.request('?',0,0)  #- не работает
device.request('MAXID DATA',0,0)  #- не работает
#x = input()

delay = 100
start = device.MAXID - delay
fin = device.MAXID

#device.request('FETCH DATA',start,fin)  #-  не работает, перевести байты в стринги

device.request('$AE33:D'+str(delay),0,0)  #-  то же самое

device.request('CLOSE',0,0)
device.unconnect()

device.plot_from_excel_file(device.xlsfilename)
device.write_path_file()

