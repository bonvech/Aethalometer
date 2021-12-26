***********#
#
#
#

## import modules
import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import datetime, date, time
from urllib.request import urlopen

import sys
import socket
import msvcrt, sys


f = open('AE33_log.txt','a')

print(sys.version)
print(datetime.now())

f.write(sys.version +'\n')
f.write(str(datetime.now()) + '\n')



#  socket.socket(family=AF_INET,type=SOCK_STREAM,proto=0,fileon=None)

##sock = socket.socket(socket.AF_INET,socket.SOCK_RAW | socket.Sock_NONBLOCK);
##
##sock.connect(('192.168.1.2',8002))
##
##sock.send('HELLO')
##data = sock.recv(1024)
##print data
##sleep(1)
##
##sock.send('MINID')
##data = sock.recv(1024)
##print data
##sleep(1)
##
##sock.send('MAXID')
##data = sock.recv(1024)
##print data
##sleep(1)
##
##sock.send('FETCH  DATA  MAXID-10  MAXID')
##data = sock.recv(1024)
##print data
##sleep(1)
##
##sock.send('CLOSE')
##data = sock.recv(1024)
##print data
##sleep(1)
##
##sock.close()


#### parse html and find 'AirCharts.init' chart 
##soup = BeautifulSoup(html, 'html.parser')
###print (soup)
##for link in soup.find_all('script'):
###    print(link)
###    w = link.get_text()
##    w = str(link)
## #   w = link
## #   print(w)
##    if 'AirCharts' in w:
##        chart = w    
## #       print(w)
##        break
##




#AE33>
chart = 'AE33-S02-00262|5|6/13/2019 7:10:00 AM|6/13/2019 7:11:00 AM|3|6/13/2019 7:08:13 AM|266|100|328|91|61|186|231|60|268|96|61|189|123|60|204|201|29|219|200|21|210| 0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0.0001|0.0001|0.0001|0.0001|0.0001|0.0001|0. 0001|0|101325.0|21.1|2328|-228|2100|30.0|40.0|30.0|0|10|40|11|17|1|214|0|28498|0'

###AE33>
##chart = 'AE33-S02-00262|6|6/13/2019 7:11:00 AM|6/13/2019 7:12:00 AM|3|6/13/2019 7:08:13 AM|905682|703266|692379|904997|689248|728780|904516|722026|717661|900277|7 36239|746235|904312|786395|778317|760497|932397|934563|820462|933478|93720 0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0.0001|0.0001|0.0001|0.0001|0.0001|0.0001| 0.0001|0|101325.0|21.1|1754|342|2096|30.0|41.0|31.0|0|10|10|12|1|1|215|0|28498|0'
##
###AE33>
##chart = 'AE33-S02-00262|7|6/13/2019 7:12:00 AM|6/13/2019 7:13:00 AM|3|6/13/2019 7:08:13 AM|907504|704410|693750|907106|690848|730375|905399|722782|718302|900296|7 36199|745824|905658|787481|779203|761569|933867|935652|821825|934942|93865 7|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0.0001|0.0001|0.0001|0.0001|0.0001|0.0001| 0.0001|0|101325.0|21.1|1700|393|2093|30.0|40.0|31.0|0|10|10|12|1|1|215|1|28498|0' 

print ('chart=',chart)

# make dict from text chart
start = chart.find('AE33-S02-00262|')
start = chart.find('|')
stop = chart[: chart.find('{"month')].rfind(',')
#comm = 'd = ' + chart[start : stop]
#comm = 'd = ' + chart[(start+1):]
comm = chart[(start+1):]
#comm = comm.replace('null', '0')
#exec(comm)
print ('comm=',comm)

#translation_map = str.maketrans('|',',')
excstr = '[' + comm.replace('|','],[') + ']'
print ('exc_str=',excstr)

#print(type(d), list(key for key in d.keys()))

# collect data to table
rows_list = [ClassName,Data,DateOrder,ExtDeviceData,ExtDeviceSetup,FilterSet,
Log,Message,Method,NDTest,Setup,TestReports[]]



# collect data to table
##rows_list = []
###for i in range(len(d['units']['h']['CH4']['data'])):
##for i in range(len(excstr)):
##    array = []
###    sectime = d['units']['h']['CH4']['data'][i][0] // 1000
## #   sectime = d[i][0] // 1000
###    dt = datetime.utcfromtimestamp(sectime)
##    #print(i, sectime, dt.strftime("%d.%m.%Y"), dt.strftime("%H"), end=' ')
###    array.append(sectime)
###    array.append(dt.strftime("%d.%m.%Y"))
###    array.append(int(dt.strftime("%H")))
##    #print(dt)
###    for param in d['units']['h'].keys():
##    for param in excstr.keys():
##        #print(d['units']['h'][param]['data'][i][1], end=' ')
###        array.append(d['units']['h'][param]['data'][i][1])
##        array.append(excstr[i][1])
##    print(array)
##    rows_list.append(array)


for i in range(len(excstr)):
    array = []
#    sectime = d['units']['h']['CH4']['data'][i][0] // 1000
 #   sectime = d[i][0] // 1000
#    dt = datetime.utcfromtimestamp(sectime)
    #print(i, sectime, dt.strftime("%d.%m.%Y"), dt.strftime("%H"), end=' ')
#    array.append(sectime)
#    array.append(dt.strftime("%d.%m.%Y"))
#    array.append(int(dt.strftime("%H")))
    #print(dt)
#    for param in d['units']['h'].keys():
#    for param in excstr.keys():
        #print(d['units']['h'][param]['data'][i][1], end=' ')
#        array.append(d['units']['h'][param]['data'][i][1])
array.append(excstr)
print('   array=',array)
#rows_list.append(array)


## записать данные в dataframe
colnames = d['units']['h'].keys()
colnames = ['timestamp', 'date', 'hour'] + list(colnames)
df = pd.DataFrame(rows_list, columns=colnames)

## read existing xls file and add new data to dataframe from file
filename = 'AE33.xlsx'
try:
    df0 = pd.read_excel(filename) #, index_col=0)
    # выбрать новые строки
    df1 = df0.append(df).drop_duplicates(keep=False)
    # добавить новые строки к старым из файла
    df = df0.append(df1, ignore_index=True).drop_duplicates()
    print(df.shape[0] - df0.shape[0], "new lines added to", filename)

    f.write(str(df.shape[0] - df0.shape[0]) + "  new lines added to  " + filename + '\n')
except:
    print("Excel file", filename, "not found")
    print(df.shape[0], "lines writen to file to ", filename)

## save results to excel file
df.set_index('timestamp').to_excel(filename)






f.close()



if msvcrt.kbhit(): #если нажата клавища
    k = ord(msvcrt.getch()) #считываем код клавиши
    if k == 27: # если клавиша Esc
        sys.exit() # завершаем программу



##sock.send('?')
##data = sock.recv(1024)
##print data
##sleep(1)
##
##sock.send('SN')
##data = sock.recv(1024)
##print data
##sleep(1)
##
##sock.send('TYPE')
##data = sock.recv(1024)
##print data
##sleep(1)
##
##sock.send('STREAM')
##data = sock.recv(1024)
##print data
##sleep(1)
##
##sock.send('INFO')
##data = sock.recv(1024)
##print data
##sleep(1)
##
##sock.send('DETAILS')
##data = sock.recv(1024)
##print data
##sleep(1)
##
##sock.send('CONTROL')
##data = sock.recv(1024)
##print data
##sleep(1)
##
##sock.send('STATION_INFO')
##data = sock.recv(1024)
##print data
##sleep(1)
##
##sock.send('$AE33')
##data = sock.recv(1024)
##print data
##sleep(1)


