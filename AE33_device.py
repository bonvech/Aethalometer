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
import config


class AE33_device:
    def __init__(self):
        self.MINID = 0
        self.MAXID = 0

        ## for data files
        self.yy = '0'        ##  year for filename of raw file
        self.mm = '0'        ## month for filename of raw file
        self.yy_D = '0'      ##  year for filename of D-file
        self.mm_D = '0'      ## month for filename of D-file 
        self.pathfile = ''   ## work directory name
        self.xlsfilename = ''      ## exl file name
        self.file_raw = None       ## file for raw data
        self.file_format_D = None  ## file for raw data
        self.file_header = ''
        self.head = ''
        self.ae_name = ''    ## 'AE33-S09-01249' # 'AE33-S08-01006'

        self.run_mode = 0
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


    ## ----------------------------------------------------------------
    ##  Print message to logfile
    ## ----------------------------------------------------------------
    def print_message(self, message, end=''):
        print(message)
        flog = open(self.logfilename,'a')
        flog.write(message + end)
        flog.close()


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
    ## read "PATHFILES.CNF"
    ############################################################################
    def read_path_file(self):
        # check file
        #print("read file")
        try:
            f = open("PATHFILES.CNF")
        except:
            print("Error!! No file PATHFILES.CNF\n\n")
            return -1

        params = [x.replace('\n','') for x in f.readlines() if x[0] != '#']
        f.close()
        #print(params)

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
                self.pathfile = param
                #print(self.pathfile)
                if not os.path.isdir(param):
                    os.makedirs(param)
                os.system("mkdir " + param)
                
                if self.pathfile[-1] != self.sep:
                    print('add sep', self.pathfile)
                    self.pathfile += self.sep       

                path = self.pathfile + 'raw' + self.sep
                #path = self.pathfile + 'raw/'
                #print(path)
                #os.system("mkdir " + path)
                if not os.path.isdir(path):
                    os.makedirs(path)

                #path = self.pathfile + '/ddat/'
                path = self.pathfile + 'ddat' + self.sep
                os.system("mkdir " + path)
                if not os.path.isdir(path):
                    os.makedirs(path)

                #path = self.pathfile + '/table/'
                path = self.pathfile + 'table' + self.sep
                os.system("mkdir " + path)
                if not os.path.isdir(path):
                    os.makedirs(path)


    # \todo ПОПРАВИТЬ в конфигурацилонном файле СЛЕШИ В ИМЕНИ ДИРЕКТОРИИ  !!!   для ВИНДА


    ############################################################################
    ############################################################################
    def write_path_file(self):
        f = open("PATHFILES.CNF.bak", 'w')
        f.write("#\n# Programm mode:\n")
        f.write("#   1 - for MAIN-menu,  0 - Auto RUN\n")
        f.write("#\n")
        f.write("RUN=" + str(self.run_mode) + "\n")
        f.write("#\n# Directory for DATA:\n")
        f.write("#\n")
        f.write(self.pathfile + '\n')
        f.write("#\n# AE33:   IP address and Port:\n")
        f.write("#\n")
        f.write("IP=" + self.IPname + '  ' + str(self.Port) +"\n")
        f.write("#\n# AE33:  Last Records:\n")
        f.write("#\n")
        f.write("MINID=" + str(self.MAXID) + "\n")
        f.write("#\n")
        f.close()


    ############################################################################
    ############################################################################
    def print_params(self):
        print("RUN = ", self.run_mode)
        print("IP = ", self.IPname)
        print("Port = ", self.Port)
        print("pathfile = ", self.pathfile)
        print("MINID = ", self.MINID)


    ############################################################################
    ############################################################################
    def connect(self):
        text = "\n============================================\n" + str(datetime.now()) + '  '
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
        ## \todo проверить, что связь установлена
        except TimeoutError:
            errcode = 1
            text = f"Message: Timeout error: AE33 on address {self.IPname} does not responde"
            bot = telebot.TeleBot(config.token, parse_mode=None)
            bot.send_message(config.channel, text)
            ## write to logfile
            text = str(datetime.now()) + ' ' + text
            self.print_message(text, '\n')

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
                print('qq, read buf(bytes)=',len(buf))
                #print(buf)

        if attempts >= 10:
            print("request: Error in receive")
            self.sock.unconnect()
            return 2

        ## --- parse buffer
        buff2 = buf.decode("UTF-8")
        buff2 = buff2.split("\r\nAE33>")
        #print('qq2,  buff2=', len(buff2), buff2)

        self.buff = buff2[0]
        #if "HELLO" in command:
            #self.buff = buff2[1]
        #else:
            #self.buff = buff2[0]

        #print('qqqq3,  self.buff=', len(self.buff))
        #print(self.buff)
        #self.buff = self.buff.split("AE33>")
        #print(self.buff)


        ### --- operate with command
        if "HELLO" in command:
            self.extract_device_name()
        if "MAXID" in command:
            self.MAXID = int(self.buff)
            print(self.MAXID)
        if "MINID" in command:
            #self.buff = self.buff.split("AE33>")
            #print(self.buff)
            self.MINID = int(self.buff)
            print(self.MINID)
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
            if "AE33:D":
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



    ############################################################################
    ############################################################################
    def parse_raw_data(self):
        print('raw data:  ')
        if len(self.buff) < 10:
            return
        self.buff = self.buff.replace("AE33>","")
        print(self.buff)

##        for line in self.buff:
##          #  print('line = ',line)
##            #if len(line) < 50:
##             #   continue
##            print('line = ',line)
##            mm, dd, yy = line.split("|")[2][:10].split('/')
##            #mm, dd, yy = self.buff.split("|")[2][:10].split('/')
##            print('m, dd, yy = ',mm,dd,yy)
##            if mm != self.mm or yy != self.yy:
##                filename = '_'.join((yy, mm)) + '_AE33-S08-01006.raw'
##                filename = self.pathfile +'\\raw\\' + filename
##                print(filename)
##                if self.file_raw:
##                    self.file_raw.close()
##                self.file_raw = open(filename, "a")
##            self.file_raw.write(self.buff+'\n')
##            #self.file_raw.write('\n')


        #mm, dd, yy = self.buff.split("|")[2][:10].split('/')
        mm, dd, yy = self.buff.split("|")[2].split(" ")[0].split('/')
        print('m, dd, yy = ',mm,dd,yy)
        if mm != self.mm or yy != self.yy:
            filename = '_'.join((yy, mm)) + '_AE33-S08-01006.raw'
            #filename = self.pathfile +'\\raw\\' + filename
            filename = self.pathfile +'/raw/' + filename
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
                filename = self.pathfile +'\wdat\\' + filename
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
            f = open(filename, 'a')
            f.write(line+'\n')
            f.close()


##        ## ##### write dataframe to excel file
##        ## make dataFrame from list
##        excel_columns = ['Date', 'Time (Moscow)', 'BC1', 'BC2', 'BC3', 'BC4', 'BC5', 'BC6',
##            'BC7', 'BB (%)', 'BCbb', 'BCff', 'Date.1', 'Time (Moscow).1']
##        dataframe_from_buffer = pd.DataFrame(rows_list, columns=excel_columns[:-4])
##        ## add columns
##        dataframe_from_buffer['BCbb'] = dataframe_from_buffer['BB (%)'].astype(float) * dataframe_from_buffer['BC5'].astype(float) / 100
##        dataframe_from_buffer['BCff'] = (100 - dataframe_from_buffer['BB (%)'].astype(float)) / 100 *  dataframe_from_buffer['BC5'].astype(float)
##        dataframe_from_buffer['Date.1'] = dataframe_from_buffer['Date']
##        dataframe_from_buffer['Time (Moscow).1'] = dataframe_from_buffer['Time (Moscow)']
##        print(dataframe_from_buffer.head())
##
##        ##### excel file #####
##        xlsfilename = yy + '_AE33-S08-01006.xlsx'
##        xlsfilename = self.pathfile + 'tableW/' + xlsfilename
##        self.xlsfilename = xlsfilename
##        ## read or cleate datafame
##        xlsdata = self.read_dataframe_from_excel_file(xlsfilename)
##        print(xlsdata.head())
##        if xlsdata.shape[0]:
##            dropset = ['Date', 'Time (Moscow)']
##            xlsdata = xlsdata.append(dataframe_from_buffer, ignore_index=True).drop_duplicates(subset=dropset, keep='last')
##            #print("Append:", xlsdata)
##            xlsdata.set_index('Date').to_excel(xlsfilename, engine='openpyxl')
##        else:
##            print("New data:")
##            dataframe_from_buffer.set_index('Date').to_excel(xlsfilename, engine='openpyxl')
##            #dataframe_from_buffer.to_excel(xlsfilename, engine='openpyxl')


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
                if self.pathfile[-1] != self.sep:
                    self.pathfile = self.pathfile + self.sep
                filename = self.pathfile + 'ddat' + self.sep + filename
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
                    f = open(filename, 'a')
                    f.write(self.file_header.replace("BB(%)", "BB (%)"))
                    f.close()
                    text = 'NOT FILE' + filename + "found. New file created."
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

            ## write to file
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
        #print("write_dataframe_to_excel_file")
        self.write_dataframe_to_excel_file(dataframe_from_buffer[self.xlscolumns])


    ############################################################################
    ### write dataframe to excel file
    ### \return - no return
    ###
    ############################################################################
    def write_dataframe_to_excel_file(self, dataframe):
        """ write dataframe to excel file """
        print("write_dataframe_to_excel_file")
        
        ## add columns
        dataframe.loc[:,'BCbb'] = dataframe.loc[:,'BB(%)'].astype(float) / 100 \
                                * dataframe.loc[:,'BC5'].astype(float)
        dataframe['BCff'] = (100 - dataframe['BB(%)'][:].astype(float)) / 100 \
                                * dataframe['BC5'][:].astype(float)

        #### extract year and month from data
        year_month = dataframe['Datetime'].apply(select_year_month).unique()

        ### prepare directory
        table_dirname = self.pathfile + 'table' + self.sep
        print("table_dirname:", table_dirname)
        if not os.path.isdir(table_dirname):
            os.makedirs(table_dirname)


        #### write to excel file
        for ym_pattern in year_month:
            #print(ym_pattern, end=' ')
            filenamexls = table_dirname + ym_pattern + '_' + self.ae_name + ".xlsx"
            filenamecsv = table_dirname + ym_pattern + '_' + self.ae_name + ".csv"
            self.xlsfilename = filenamexls

            ## отфильтровать строки за нужный месяц и год
            dfsave = dataframe[dataframe['Datetime'].apply(select_year_month) == ym_pattern]
            print(ym_pattern, ": ", dfsave.shape)

            ##### try to open excel file #####
            ## read or create datafame
            xlsdata = self.read_dataframe_from_excel_file(filenamexls)
            if xlsdata.shape[0]:  ## data was read from file
                dfsave = pd.concat([xlsdata, dfsave], ignore_index=True)\
                        .drop_duplicates(subset=['Datetime'])\
                        .sort_values(by=['Datetime'])
                if xlsdata.shape[0] == dfsave.shape[0]:
                    text = f"No new data received from {self.ae_name}"
                    bot = telebot.TeleBot(config.token, parse_mode=None)
                    bot.send_message(config.channel, text)
                    self.print_message(text, '\n')
                    return 1
            ## no data was read - no file was opened
            elif os.path.file(filenamexls): ## if file exists, but not read
                text = "Data file " + filenamexls + " is not available. File with new name will created."
                self.print_message(text, '\n')                
                timestr = "_".join(str(datetime.now()).split())
                filenamexls = filenamexls[:-4] + timestr + ".xlsx"
                filenamecsv = filenamecsv[:-3] + timestr + ".csv"
                text = "Data file " + filenamexls + " created."
                self.print_message(text, '\n')
            else:
                text = "else: " + str(os.path.file(filenamexls))
                self.print_message(text, '\n')


            dfsave.set_index('Datetime').to_excel(filenamexls, engine='openpyxl')
            dfsave.set_index('Datetime').to_csv(filenamecsv)
            return 0


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
        current_ae_name = [x.split()[-1] for x in header if 'AE33' in x][0]
        if self.ae_name == '':
            self.ae_name = current_ae_name
        if self.ae_name != current_ae_name:
            print("Current device has different name:", current_ae_name)
        #print(self.ae_name)


        ## --- read data
        columns = self.head.split("; ")[:-1] + [str(i) for i in range(1, 10)]
        #print(columns)
        #print(len(columns), end=' ')
        datum = pd.read_csv(filename, sep='\s+', on_bad_lines='warn', 
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
