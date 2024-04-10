## you need to install 
# pip install pandas
# pip install openpyxl==3.0.9
# pip install pyTelegramBotAPI

import sys
import socket
import time
import datetime
from   datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
import os
# for bot
import telebot

import telebot_config


## ----------------------------------------------------------------
##  
## ----------------------------------------------------------------
def select_year_month(datastring):
    return datastring.split()[0][-4:] + '_' + datastring[3:5]


## ----------------------------------------------------------------
##  
## ----------------------------------------------------------------
def get_local_ip():
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    return hostname, local_ip



############################################################################
############################################################################
class AE33_device:
    def __init__(self):
        self.MINID = 0
        self.MAXID = 0

        ## for data files
        self.yy = '0'        ##  year for filename of raw file
        self.mm = '0'        ## month for filename of raw file
        self.yy_D = '0'      ##  year for filename of D-file
        self.mm_D = '0'      ## month for filename of D-file 
        self.datadir = ''   ## work directory name
        self.xlsfilename = ''      ## exl file name
        self.csvfilename = ''      ## csv file name
        self.file_raw = None       ## file for raw data
        self.file_format_D = None  ## file for raw data
        self.file_header = ''
        self.head = ''
        self.ae_name = ''    ## 'AE33-S09-01249' # 'AE33-S08-01006'

        self.run_mode = 0
        self.logdirname = ""
        self.logfilename = "ae33_log.txt"  ## file to write log messages


        self.buff = ''
        self.info = ''
        self.IPname = '192.168.1.62'
        self.IsConnected = 1
        self.Port = 8002  ## port number
        self.sock = None  ## socket
        self.xlscolumns = ['Datetime', 'BC1', 'BC2', 'BC3', 'BC4', 'BC5', 'BC6', 'BC7', 'BB(%)']

        if 'ix' in os.name:
            self.sep = '/'  ## -- path separator for LINIX
        else:
            self.sep = '\\' ## -- path separator for Windows

        ## --- run functions ---
        #sock = socket.socket()
        self.fill_header() ## to develop data files
        self.read_config_file()
        self.print_params()
        self.prepare_dirs()
        
        #self.logfilename = self.logdirname + "_".join(["_".join(str(datetime.now()).split('-')[:2]), self.ae_name,  'log.txt'])
        #self.logfilename =  + ym_pattern + '_' + self.ae_name + "log.txt"


    ## ----------------------------------------------------------------
    ##  Print message to logfile
    ## ----------------------------------------------------------------
    def print_message(self, message, end=''):
        print(message)
        self.logfilename = self.logdirname + "_".join(["_".join(str(datetime.now()).split('-')[:2]), self.ae_name,  'log.txt'])
        with open(self.logfilename,'a') as flog:
            flog.write(str(datetime.now()) + ':  ')
            flog.write(message + end)


    ## ----------------------------------------------------------------
    ##  write message to bot
    ## ----------------------------------------------------------------
    def write_to_bot(self, text):
        try:
            hostname, local_ip = get_local_ip()
            text = f"{hostname} ({local_ip}): {text}"
            
            bot = telebot.TeleBot(telebot_config.token, parse_mode=None)
            bot.send_message(telebot_config.channel, text)
        except Exception as err:
            ##  напечатать строку ошибки
            text = f": ERROR in writing to bot: {err}"
            self.print_message(text)  ## write to log file


    ############################################################################
    ############################################################################
    def fill_header(self):
        self.head = "Date(yyyy/MM/dd); Time(hh:mm:ss); Timebase; RefCh1; Sen1Ch1; Sen2Ch1; RefCh2; Sen1Ch2; Sen2Ch2; RefCh3; Sen1Ch3; Sen2Ch3; RefCh4; Sen1Ch4; Sen2Ch4; RefCh5; Sen1Ch5; Sen2Ch5; RefCh6; Sen1Ch6; Sen2Ch6; RefCh7; Sen1Ch7; Sen2Ch7; Flow1; Flow2; FlowC; Pressure(Pa); Temperature(°C); BB(%); ContTemp; SupplyTemp; Status; ContStatus; DetectStatus; LedStatus; ValveStatus; LedTemp; BC11; BC12; BC1; BC21; BC22; BC2; BC31; BC32; BC3; BC41; BC42; BC4; BC51; BC52; BC5; BC61; BC62; BC6; BC71; BC72; BC7; K1; K2; K3; K4; K5; K6; K7; TapeAdvCount; "
        #self.ddat_head = self.head.replace("BB(%)", "BB (%)")
        if self.ae_name == '':
            print("\n\nfill_header:  WARNING! Name of the device is unknown!!!!!\n")
        self.file_header = (
            "AETHALOMETER\n" +
            "Serial number = " +
            self.ae_name +  # "AE33-S08-01006" +
            "\n" + 
            "Application version = 1.6.7.0\nNumber of channels = 7\n\n" + 
            self.head + "\n\n\n")


    ############################################################################
    ## read config file
    ############################################################################
    def read_config_file(self):
        # read file
        try:
            import ae33_config as config
        except Exception as err:
            ##  напечатать строку ошибки
            text = f": ERROR in reading config: {err}"
            print(text)
            print(f"\n!!! read_config_file Error!! Check configuration file 'ae33_config' in  \n\n")
            return -1
            
        
        self.IPname = config.IP
        self.Port   = config.Port
        self.MINID  = config.MINID
        self.MAXID  = config.MINID
        
        self.datadir = config.Datadir.strip()
        self.datadir = self.sep.join(self.datadir.split("/"))
        ##  add separator to end of dirname
        if self.datadir[-1] != self.sep:
            #print('add sep', self.datadir)
            self.datadir += self.sep
        
        if not self.ae_name:        
            self.ae_name = config.ae_name
            self.fill_header()

        self.write_config_file()


    ############################################################################
    ## read config file "PATHFILES.CNF"
    ############################################################################
    def read_path_file(self):
        # check file
        try:
            #f = open("PATHFILES.CNF")
            f = open("ae33_config.py")
            params = [x.replace('\n','') for x in f.readlines() if x[0] != '#']
            f.close()
        except:
            print("Error!! No config file PATHFILES.CNF\n\n")
            return -1

        for param in params:
            if "RUN" in param:
                self.run_mode = int(param.split('=')[1])
            elif "IP" in param:
                self.IPname = param.split('=')[1].split()[0]
                self.Port   = int(param.split('=')[1].split()[1])
            elif "MINID" in param:
                self.MINID = int(param.split('=')[1])
                self.MAXID = self.MINID
            else:
                self.datadir = param
                ##  add separator to end of dirname
                if self.datadir[-1] != self.sep:
                    print('add sep', self.datadir)
                    self.datadir += self.sep
                

    ############################################################################
    ##  check and create dirs for data 
    ############################################################################
    def prepare_dirs(self):
        if not os.path.isdir(self.datadir):
            os.makedirs(self.datadir)
        
        path = self.datadir + 'raw' + self.sep
        #print(path)
        if not os.path.isdir(path):
            os.makedirs(path)

        path = self.datadir + 'ddat' + self.sep
        if not os.path.isdir(path):
            os.makedirs(path)

        path = self.datadir + 'table' + self.sep
        if not os.path.isdir(path):
            os.makedirs(path)

        path = self.datadir + 'log' + self.sep
        if not os.path.isdir(path):
            os.makedirs(path)
        self.logdirname = path


    ############################################################################
    ############################################################################
    def write_config_file(self):
        #f = open("PATHFILES.CNF.bak", 'w')
        f = open("ae33_config.bak", 'w')
        f.write(f'#\n Device name')
        f.write(f'ae_name= "{self.ae_name}"\n')
        f.write( "#\n# Directory for DATA:\n#\n")
        f.write(f'Datadir = "{self.datadir}"\n')
        f.write( "#\n# AE33:   IP address and Port:\n#\n")
        f.write(f'IP = "{self.IPname}"\n') 
        f.write(f"Port = {self.Port}\n")
        f.write( "#\n# AE33:  Last Records:\n#\n")
        f.write(f"MINID = {self.MAXID}\n")
        f.write( "#\n")
        f.close()


    ############################################################################
    ############################################################################
    def print_params(self):
        #print(f"RUN = ",      self.run_mode)
        print(f"ae_name = ",  self.ae_name)
        print(f"IP = ",       self.IPname)
        print(f"Port = ",     self.Port)
        print(f"datadir = ",  self.datadir)
        print(f"MINID = ",    self.MINID)


    ############################################################################
    ############################################################################
    def connect(self):
        text = "\n============================================\n"
        self.print_message(text)

        errcode = 0
        #if self.active == -1:
        #    return -1

        ## --- create socket
        #socket.socket(family='AF_INET', type='SOCK_STREAM', proto=0, fileno=None)
        #self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM | socket.SOCK_NONBLOCK)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #self.sock = socket.socket()

        ## --- connect to server
        try:
            self.sock.connect((self.IPname, self.Port))
            text = 'socket connected'
            self.print_message(text, '\n')
            #sock.connect(('localhost', 3000)) 
        except Exception as e:  #TimeoutError:
            errcode = 1
            text = f"Message: error <<{e}>>: {self.ae_name} on address {self.IPname} does not responde"
            ## write to logfile
            self.print_message(text, '\n')
            ## write to bot
            self.write_to_bot(text)
           
        return errcode


    ############################################################################
    ############################################################################
    def unconnect(self):
        ##  socket.shutdown(self.sock, SHUT_RD|SHUT_WR)
        self.sock.close()


    ############################################################################
    ### send command to device, read answer and operate with it
    ### \return 0 - OK
    ###         1 - command CLOSE
    ###         2 - error in receive data
    ############################################################################
    def request(self, command, start, stop):
        if command == 'FETCH DATA':
            command += ' ' + str(start) + ' ' + str(stop)
        command += '\r\n'
        print(command, end='')

        ## --- send command ---
        time.sleep(1)
        ##self.sock.send(bytes(command))
        bytes = self.sock.send(command.encode())
        print(bytes, end='>   ')
        ## \todo проверить, что все отправилось
        if bytes != len(command):
            print("request: Error in sending data!! ") 
        print('sent ', bytes, 'bytes to the socket')

        if "CLOSE" in command:
            return 1

        ## --- 10 attempts to read buffer
        attempts = 0
        buf = ''
        while(len(buf) == 0 and attempts < 10):
            time.sleep(1)
            attempts += 1
            buf = self.sock.recv(2000000)
            if len(buf) == 0:
                print('not data,  buf lenght=', len(buf), ' attempt=', attempts)
            else:
                print('qq, read buf(bytes)=', len(buf))
                #print(buf)

        if attempts >= 10:
            print("request: Error in receive")
            self.sock.unconnect()
            return 2

        ## --- parse buffer
        buff2 = buf.decode("UTF-8")
        if   "AE33" in buff2:
            buff2 = buff2.split("\r\nAE33>")
        elif "AE43" in buff2:
            buff2 = buff2.split("\r\nAE43>")
        #buff2 = buff2.split("\r\nAE33>")
        #print('qq2,  buff2=', len(buff2), buff2)

        self.buff = buff2[0]

        ### --- operate with command
        if "HELLO" in command:
            self.extract_device_name()
        if "MAXID" in command:
            self.MAXID = int(self.buff)
            print(f"MAXID = {self.MAXID}")
        if "MINID" in command:
            #self.buff = self.buff.split("AE33>")
            self.MINID = int(self.buff)
            print(f"MINID = {self.MINID}")
        if '?' in command:
            self.info = self.buff
        if "FETCH" in command:
            i = 0
            while(i < (len(buff2) - 1)):
                print('ii=',i)
                self.buff = buff2[i]
                self.parse_raw_data()
                i += 1
        if "AE33" in command:
            if "AE33:D" in command:
                print("'AE33:D' in command")
                self.parse_format_D_data()
        #    if "AE33:W":
        #        self.parse_format_W_data()

        return 0


    ############################################################################
    ############################################################################
    def extract_device_name(self):
        buff = self.buff.split("\r\n")
        self.ae_name = [x.split()[2] for x in buff if "serialnumb" in x][0]
        text = f'Device name: {self.ae_name}'
        self.print_message(text, '\n')
        self.fill_header()


    ############################################################################
    ############################################################################
    def parse_raw_data(self):
        print('raw data:  ')
        if len(self.buff) < 10:
            return
        self.buff = self.buff.replace("AE33>","")
        print(self.buff)

        #mm, dd, yy = self.buff.split("|")[2][:10].split('/')
        mm, dd, yy = self.buff.split("|")[2].split(" ")[0].split('/')
        print('m, dd, yy = ',mm,dd,yy)
        if mm != self.mm or yy != self.yy:
            filename = self.datadir + self.sep + 'raw' + self.sep + filename
            print(filename)
            if self.file_raw:
                self.file_raw.close()
            self.file_raw = open(filename, "a")
        self.file_raw.write(self.buff+'\n')
            #self.file_raw.write('\n')

        self.file_raw.flush()
        self.mm = mm
        self.yy = yy


    ############################################################################
    ############################################################################
    def parse_format_W_data(self):
        ## main
        #print('qqqqqqqqqqq')
        if len(self.buff) < 10:
            return
        #self.buff = self.buff.split("AE33>")
        if 'ix' in os.name:
            self.buff = self.buff.split("\n")  ## for Linux
        else:
            self.buff = self.buff.split("\r\n") ## for Windows

        lastmm, lastyy = '0', '0'
        filename = ''
        lastline = ''
        need_check = True
        dateformat = "%Y/%m/%d %H:%M:%S"
        #print('lines:')
        #print(self.buff)

        ## for excel data
        header = self.file_header[self.file_header.find("Date"):].split("; ")
        ## column names to write to file
        columns = ['Date(yyyy/MM/dd)', 'Time(hh:mm:ss)', 'BC1', 'BC2', 'BC3', 'BC4', 'BC5', 'BC6', 'BC7', 'BB(%)']
        ## record numbers of columns to write to file
        colnums = [header.index(x) for x in columns]
        rows_list = []

        for line in self.buff[::-1]:
            #print('line:   ',line)
            yy, mm, _ = line.split()[0].split('/')
            #print(yy, mm)

            # for first line or new file
            if mm != lastmm or yy != lastyy:
                ##### ddat file 
                filename = '_'.join((yy, mm)) + '_AE33-S08-01006.wdat'
                filename = self.datadir + self.sep + 'wdat' + self.sep + filename
                print(filename,mm,yy,lastmm,lastyy)
                try:
                    ## ddat file exists
                    f = open(filename, 'r')
                    lastline = f.readlines()[-1].split()
                    #print(lastline)
                    f.close()
                    print('3')
                    lasttime = lastline[0] + ' ' + lastline[1]
                    print('1  ',lasttime)
                    lasttime = datetime.strptime(lasttime, dateformat)
                    print('4',lastmm,lastyy,mm,yy)
                    need_check = True
                except:
                    ## no file: put header into file
                    print('NOT FILE', filename)
                    f = open(filename, 'a')
                    f.write(self.file_header)
                    f.close()
                    lastline = []
                    need_check = False 
                lastmm = mm
                lastyy = yy

            ## add line data to dataframe 
            line_to_dataframe = [line.split()[i] for i in colnums]
            #print("line_to_dataframe:>",line_to_dataframe)
            line_to_dataframe = line_to_dataframe[:2]\
                                + [int(x) for x in line_to_dataframe[2:-1]]\
                                + [float(line_to_dataframe[-1])]
            rows_list.append(line_to_dataframe)
            #print(rows_list)


            ## check line to be added to datafile
            if need_check: # and len(lastline):
                #print(line)
                nowtime  = line.split()[0] + ' ' + line.split()[1]
                #print(nowtime)
                nowtime  = datetime.strptime(nowtime,  dateformat)
                print(nowtime - lasttime)
                ## if line was printed earlier
                if nowtime <= lasttime:
                    continue

            need_check = False

            ## write to file
            with open(filename, 'a') as f:
                f.write(line+'\n')


    ############################################################################
    ## write to ddat data
    ############################################################################
    def parse_format_D_data(self):
        ## main
        if len(self.buff) < 10:
            return

        #self.buff = self.buff.split("AE33>")
        if 'ix' in os.name:
            self.buff = self.buff.split("\n")   ## for Linux
        else:
            self.buff = self.buff.split("\r\n") ## for Windows

        lastmm, lastyy = '0', '0'
        filename = ''
        lastline = ''
        need_check = True
        dateformat = "%Y/%m/%d %H:%M:%S"
        #print('lines:')
        #print(self.buff)

        ## --- for excel data
        if self.file_header == '':
            self.fill_header()
        header = self.file_header[self.file_header.find("Date"):].split("; ")
        #columns = ['Date(yyyy/MM/dd)', 'Time(hh:mm:ss)', 'BC1', 'BC2', 'BC3', 'BC4', 'BC5', 'BC6', 'BC7', 'BB (%)']
        columns = ['Date(yyyy/MM/dd)', 'Time(hh:mm:ss)', 'BC1', 'BC2', 'BC3', 'BC4', 'BC5', 'BC6', 'BC7', 'BB(%)']
        colnums = [header.index(x) for x in columns]
        rows_list = []


        ## ---- write from buffer to ddat file --- ##
        for line in self.buff[::-1]:
            #print('line:   ',line)
            yy, mm, _ = line.split()[0].split('/')
            #print(yy, mm)

            ## --- for first line or new file
            if mm != lastmm or yy != lastyy:
                ## -- ddat filename 
                #filename = '_'.join((yy, mm)) + "_" + 'AE33-S08-01006.ddat'
                filename = '_'.join((yy, mm)) + "_" + self.ae_name + '.ddat'
                if self.datadir[-1] != self.sep:
                    self.datadir += self.sep
                filename = self.datadir + 'ddat' + self.sep + filename
                print(filename,mm,yy,lastmm,lastyy)
                try:
                    ## ddat file exists: read last line datetime
                    f = open(filename, 'r')
                    lastline = f.readlines()[-1].split()
                    #print(lastline)
                    f.close()
                    #print('3')
                    lasttime = lastline[0] + ' ' + lastline[1]
                    print('1: ', lasttime)
                    lasttime = datetime.strptime(lasttime, dateformat)
                    need_check = True
                except:
                    ## no file: create new file and write header
                    with open(filename, 'a') as f:
                        f.write(self.file_header.replace("BB(%)", "BB (%)"))
                    text = f"New {filename} file created."
                    self.print_message(text, '\n')
                    lastline = []
                    need_check = False
                lastmm = mm
                lastyy = yy

            ## add line data to dataframe 
            line_to_dataframe = [line.split()[i] for i in colnums]
            #print("line_to_dataframe:>",line_to_dataframe)
            line_to_dataframe = line_to_dataframe[:2]\
                                + [int(x) for x in line_to_dataframe[2:-1]]\
                                + [float(line_to_dataframe[-1])]
            rows_list.append(line_to_dataframe)
            #print(rows_list)


            ## check line to be added to datafile
            if need_check: # and len(lastline):
                #print(line)
                nowtime  = line.split()[0] + ' ' + line.split()[1]
                #print(nowtime)
                nowtime  = datetime.strptime(nowtime,  dateformat)
                #print(nowtime - lasttime)
                ## if line was printed earlier
                if nowtime <= lasttime:
                    continue

            need_check = False

            ## write to ddat file
            f = open(filename, 'a')
            f.write(line+'\n')
            f.close()
        ## ---- end of write from buffer to ddat file --- ##
 
 
        print("make dataframe_from_buffer")
        dataframe_from_buffer = pd.DataFrame(rows_list, columns=columns)

        ## reformat datetime string
        dataframe_from_buffer['Datetime'] = dataframe_from_buffer['Date(yyyy/MM/dd)'].apply(lambda x: ".".join(x.split('/')[::-1])) \
                + ' ' \
                + dataframe_from_buffer['Time(hh:mm:ss)'].apply(lambda x: ':'.join(x.split(':')[:2]))

        ## --- save fo excel
        self.write_dataframe_to_excel_file(dataframe_from_buffer[self.xlscolumns])


    ############################################################################
    ### write dataframe to excel file
    ### \return - no return
    ###
    ############################################################################
    def write_dataframe_to_excel_file(self, dataframe):
        """ write dataframe to excel file """
        print("write_dataframe_to_data_files")
        
        ## add columns
        #dataframe.loc[:,'BCbb'] = dataframe.loc[:,'BB(%)'].astype(float) / 100 \
        #                        * dataframe.loc[:,'BC5'].astype(float)
        dataframe['BCbb'] = dataframe['BB(%)'][:].astype(float) / 100 \
                                * dataframe['BC5'][:].astype(float)
        dataframe['BCff'] = (100 - dataframe['BB(%)'][:].astype(float)) / 100 \
                                * dataframe['BC5'][:].astype(float)

        #### extract year and month from data
        year_month = dataframe['Datetime'].apply(select_year_month).unique()

        ### prepare directory
        table_dirname = self.datadir + 'table' + self.sep
        print("table_dirname:", table_dirname)
        if not os.path.isdir(table_dirname):
            os.makedirs(table_dirname)


        #### write to excel file
        for ym_pattern in year_month:
            #print(ym_pattern, end=' ')
            filenamexls = table_dirname + ym_pattern + '_' + self.ae_name + ".xlsx"
            filenamecsv = table_dirname + ym_pattern + '_' + self.ae_name + ".csv"
            self.xlsfilename = filenamexls
            self.csvfilename = filenamecsv

            ## отфильтровать строки за нужный месяц и год
            dfsave = dataframe[dataframe['Datetime'].apply(select_year_month) == ym_pattern]
            text = ym_pattern + ": " + str(dfsave.shape)
            self.print_message(text, '\n')
            #print(ym_pattern, ": ", dfsave.shape)

            ##### try to open excel file #####
            ## read or create datafame
            #xlsdata = self.read_dataframe_from_excel_file(filenamexls)
            csvdata = self.read_dataframe_from_csv_file(filenamecsv)
            if csvdata.shape[0]:  ## data was read from file
                dfsave = pd.concat([csvdata, dfsave], ignore_index=True)\
                        .drop_duplicates(subset=['Datetime'])\
                        .sort_values(by=['Datetime'])
                if csvdata.shape[0] == dfsave.shape[0]:
                    text = f"No new data received from {self.ae_name}"
                    self.write_to_bot(text)                    
                    self.print_message(text, '\n')
                    return 1
                text = str(csvdata.shape[0]) + " lines was read from excel file"
                self.print_message(text, '\n')
            ## no data was read - no file was opened
            elif os.path.isfile(filenamexls): ## if file exists, but not read
                text = "Data file " + filenamexls + " is not available. File with new name will created."
                self.print_message(text, '\n')                
                timestr = "_".join(str(datetime.now()).split())
                filenamexls = filenamexls[:-4] + timestr + ".xlsx"
                filenamecsv = filenamecsv[:-3] + timestr + ".csv"
                text = "Data file " + filenamexls + " created."
                self.print_message(text, '\n')
            else:
                text = f"New file {filenamexls} + {str(os.path.isfile(filenamexls))}" 
                self.print_message(text, '\n')
                text = f"New {filenamexls} created"
                self.write_to_bot(text)
                

            print(f"write to {filenamecsv}")
            dfsave.set_index('Datetime').to_csv(filenamecsv)
            print(f"write to {filenamexls}")
            dfsave.set_index('Datetime').to_excel(filenamexls, engine='openpyxl')
            return 0



    ############################################################################
    ############################################################################
    def read_dataframe_from_csv_file(self, csvfilename): ## xlsfilename
        #columns = ['Date', 'Time (Moscow)', 'BC1', 'BC2', 'BC3', 'BC4', 'BC5', 'BC6',
        #    'BC7', 'BB(%)', 'BCbb', 'BCff', 'Date.1', 'Time (Moscow).1']
        columns = self.xlscolumns
        create_new = False

        try:
            ## read csv file to dataframe
            datum = pd.read_csv(csvfilename)
            #print(datum)
            print(csvfilename, "read, df: ", datum.shape)
            print(datum.head(2))
            if datum.shape[1] != len(columns) + 2:
                text = f"WARNING!!! File {csvfilename} has old format, is ignoring!!!"
                self.print_message(text, '\n')

                point = csvfilename.rfind('.')
                os.rename(csvfilename, csvfilename[:point] + "_old" +  csvfilename[point:])
                create_new = True
        except:
            create_new = True

        if create_new:
            # create new dummy dataframe
            datum = pd.DataFrame(columns=columns)
            text = "Can not open file: " + csvfilename + "  Empty dummy dataframe created"
            self.print_message(text, '\n')

        return datum


    ############################################################################
    ############################################################################
    def read_dataframe_from_excel_file(self, xlsfilename):
        #columns = ['Date', 'Time (Moscow)', 'BC1', 'BC2', 'BC3', 'BC4', 'BC5', 'BC6',
        #    'BC7', 'BB(%)', 'BCbb', 'BCff', 'Date.1', 'Time (Moscow).1']
        columns = self.xlscolumns
        create_new = False

        try:
            ## read excel file to dataframe
            ## need to make "pip install openpyxl==3.0.9" if there are problems with excel file reading
            datum = pd.read_excel(xlsfilename)
            print(xlsfilename, "read, df: ", datum.shape)
            print(datum.head(2))
            if datum.shape[1] != len(columns) + 2:
                text = f"WARNING!!! File {xlsfilename} has old format, is ignoring!!!"
                self.print_message(text, '\n')

                point = xlsfilename.rfind('.')
                os.rename(xlsfilename, xlsfilename[:point] + "_old" +  xlsfilename[point:])
                create_new = True
        except:
            create_new = True

        if create_new:
            # create new dummy dataframe
            datum = pd.DataFrame(columns=columns)
            text = "Can not open file: " + xlsfilename + "  Empty dummy dataframe created"
            self.print_message(text, '\n')

        return datum


    ############################################################################
    ############################################################################
    ## read data from ONE ddat file
    ## \return   DataFrame with data from one ddat file
    def read_ddat_file(self, filename):
        ## --- check file exists
        if not os.path.exists(filename):
            return -1

        ## --- check numbers in line: if != 65 - print error
        lens = set(len(line.split()) for line in open(filename).readlines()[8:])
        if len(lens) > 1:
            print('!!! has line with different column numbers:', *lens, end=' ')

        ## --- check file header
        header = open(filename).readlines()[:7]
        if not any(True if self.head[:-4] in x else False for x in header):
        #if self.head[:-4] not in header:
            print("!!!!   File header differ from standart header !!!!")
            print("header:\n", header[:-4])
            print("standard header:\n", self.head[:-4])

        ## --- check device name
        current_ae_name = [x.split()[-1] for x in header if 'AE' in x][0]
        if self.ae_name == '':
            self.ae_name = current_ae_name
        if self.ae_name != current_ae_name:
            print("Current device has different name:", current_ae_name)
            self.ae_name = current_ae_name
        #print(self.ae_name)
        self.fill_header()


        ## --- read data
        columns = self.head.split("; ")[:-1] + [str(i) for i in range(1, 10)]
        #print(columns)
        #print(len(columns), end=' ')
        print(filename)
        datum = pd.read_csv(filename, sep="\s+", on_bad_lines='warn', 
                            skiprows=8,  skip_blank_lines=True,
                            index_col=None, names=columns, 
                            encoding='windows-1252')
        #print(columns, )

        ## add column to dataframe 
        #datum = datum.rename(columns={"BB (%)": "BB(%)"})
        datum['Datetime'] = datum['Date(yyyy/MM/dd)'].apply(lambda x: ".".join(x.split('/')[::-1])) \
                        + ' ' \
                        + datum['Time(hh:mm:ss)'].apply(lambda x: ':'.join(x.split(':')[:2]))
        return datum[self.xlscolumns]


    ############################################################################
    ############################################################################
    ## ---- read data from ALL ddat files ----
    ## \return DataFrame with data from all ddat file
    def read_every_month_files(self, dirname, end='.ddat'):
        ## create empty DataFrame
        data = pd.DataFrame(columns=self.xlscolumns)

        ## check directory
        if not os.path.isdir(dirname):
            print("No directory to read data exists: ", dirname)
            print("Change directorey name or put data to ", dirname)
            return data ## return empty dataframe

        # перебрать все файлы и считать из них
        year = 2022 ### \todo Add many years 
        for month in ['06']: ### \todo Add all months
            filename = dirname + f"{year}_{month}_" + self.ae_name + end 
            #print("file: ", filename, end=' ')

            ## check file exists
            if not os.path.exists(filename):
                continue

            print(filename, end=' ')

            df = self.read_ddat_file(filename)
            if type(df) == type(-1):
                continue
            print("df: ", df.shape)
            data = pd.concat([data, df], ignore_index=True)
        return data



    ############################################################################
    ############################################################################
    ## ---- read data from ALL dat files ----
    ## \return DataFrame with data from all ddat file
    def read_every_day_files(self, dirname, end='.dat'):
        ## create empty DataFrame
        data = pd.DataFrame(columns=self.xlscolumns)

        ## check directory
        if not os.path.isdir(dirname):
            print("No directory to read data exists: ", dirname)
            print("Change directorey name or put data to ", dirname)
            return data ## return empty dataframe

        # перебрать все файлы и считать из них
        year = 2022 ### \todo Add many years 
        for month in ['05', '06']: ### \todo Add all months
            for day in range(1, 32):
                filename = dirname + 'AE33_' + self.ae_name + '_' \
                         + f"{year}{month}{day:02d}" + end 

                ## check file exists
                if not os.path.exists(filename):
                    continue

                print(filename, end=' ')

                df = self.read_ddat_file(filename)
                if type(df) == type(-1):
                    continue
                print("df: ", df.shape)
                data = pd.concat([data, df], ignore_index=True)
        return data



    ############################################################################
    ############################################################################
    def plot_from_excel_file(self, xlsfilename):
        try:
            ## read excel file to dataframe
            ## need to make "pip install openpyxl==3.0.9" if there are problems with excel file reading
            datum = pd.read_excel(xlsfilename)
        except:
            print("Error! No excel data file:", xlsfilename)
            return

        fig = plt.figure(figsize=(14, 5))
        plt.plot(datum["BCff"][-2880:], 'k', label='BCff')
        plt.plot(datum["BCbb"][-2880:], 'orange', label='BCbb')
        plt.legend()
        plt.grid()
        plt.savefig('Moscow_bb.png', bbox_inches='tight')



############################################################################
############################################################################
def select_year_month(datastring):
    return "_".join([x for x in datastring.split()[0].split('.')[2:0:-1]])
    #return "_".join([x for x in datastring.split()[0].split('/')[:2]])
    #return datastring.split('/')[0] + '_' + datastring.split('/')[1]
