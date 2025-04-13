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
    #return datastring.split()[0][-4:] + '_' + datastring[3:5]
    return "_".join([x for x in datastring.split()[0].split('.')[2:0:-1]])
    #return "_".join([x for x in datastring.split()[0].split('/')[:2]])
    #return datastring.split('/')[0] + '_' + datastring.split('/')[1]


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
        self.leftspots = -1 ## -1 - many spots left, no warning will appear

        ## for data files
        self.yy = '0'        ##  year for filename of raw file
        self.mm = '0'        ## month for filename of raw file
        #self.yy_D = '0'      ##  year for filename of D-file
        #self.mm_D = '0'      ## month for filename of D-file 
        self.datadir = ''    ## work directory name
        self.xlsfilename = ''      ## exl file name
        self.csvfilename = ''      ## csv file name
        #self.file_raw = None       ## file for raw data
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

        ##  --- run functions ---
        #self.fill_header() ## to develop data files
        self.read_config_file()
        self.print_params()
        self.prepare_dirs()


    ############################################################################
    ##  Print message to logfile
    ############################################################################
    def print_message(self, message, end=''):
        print(message)
        self.logfilename = self.logdirname + "_".join(["_".join(str(datetime.now()).split('-')[:2]), self.ae_name,  'log.txt'])
        with open(self.logfilename,'a') as flog:
            flog.write(str(datetime.now()) + ':  ')
            flog.write(message + end)


    ############################################################################
    ##  write message to bot
    ############################################################################
    def write_to_bot(self, text):
        try:
            hostname, local_ip = get_local_ip()
            text = f"{hostname} ({local_ip}): {self.ae_name}: {text}"
            
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
            f"Serial number = {self.ae_name}\n" +  # "AE33-S08-01006" +
            "Application version = 1.6.7.0\n" + 
            "Number of channels = 7\n\n" + 
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
        
        ##  check and create dirs for data
        for dirname in ['ddat', 'table', 'log']: ##'raw'
            path = f"{self.datadir}{dirname}{self.sep}"
            if not os.path.isdir(path):
                os.makedirs(path)
            if dirname == 'log':
                self.logdirname = path


    ############################################################################
    ############################################################################
    def write_config_file(self):
        with open("ae33_config.bak", 'w') as f:
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


    ############################################################################
    ############################################################################
    def print_params(self):
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

        ## --- create socket
        #self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM | socket.SOCK_NONBLOCK)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #self.sock = socket.socket()

        ## --- connect to server
        errcode = 0
        try:
            self.sock.connect((self.IPname, self.Port))
            text = 'socket connected'
            self.print_message(text, '\n')
        except Exception as e:  #TimeoutError:
            errcode = 1
            text = f"Message: error <<{e}>>: {self.ae_name} on address {self.IPname} does not responde"
            self.print_message(text, '\n')  ## write to logfile
            self.write_to_bot(text)  ## write to bot
           
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
    ###         3 - error in send data
    ############################################################################
    def request(self, command, start, stop):
        if command == 'FETCH DATA':
            command += ' ' + str(start) + ' ' + str(stop)
        command += '\r\n'
        print(f"command: {command}", end='')

        ## --- send command ---
        time.sleep(1)

        bytes = self.sock.send(command.encode())
        #print(bytes, end='>   ')
        ##  проверить, что все отправилось
        if bytes != len(command):
            print("request: Error in sending data!! ")
            return 3           
        #print('sent ', bytes, 'bytes to the socket')

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
                pass
                #print('qq, read buf(bytes)=', len(buf))
                #print(buf)

        if attempts >= 10:
            print("request: Error in receive")
            self.unconnect()
            return 2

        ## --- parse buffer
        buff2 = buf.decode("UTF-8")
        if   "AE33" in buff2:
            buff2 = buff2.split("\r\nAE33>")
        elif "AE43" in buff2:
            buff2 = buff2.split("\r\nAE43>")
        else:
            print_message("ERROR! Name of device is not AE33 or AE43")

        self.buff = buff2[0]

        ### --- operate with command
        if "HELLO" in command:
            self.extract_device_name()
        if "MAXID" in command:
            self.MAXID = int(self.buff)
            print(f"MAXID = {self.MAXID}")
        if "MINID" in command:
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
            #self.write_raw_data_to_file()
            if "AE33:D" in command:
                self.parse_format_D_data()

        return 0


    ############################################################################
    ############################################################################
    def extract_device_name(self):
        buff = self.buff.split("\r\n")
        self.ae_name = [x.split()[2] for x in buff if "serialnumb" in x][0]
        text = f"Device name: {self.ae_name}"
        self.print_message(text, '\n')
        self.fill_header()


    ############################################################################
    ##  write raw data from buffer to file to check
    ############################################################################
    def write_raw_data_to_file(self):
        print('raw data:  ')
        if len(self.buff) < 10:
            return
            
        self.buff = self.buff.replace("AE33>","")
        print(self.buff)

        yy, mm, dd = self.buff.split(" ")[0].split('/')
        print('dd, mm, yy = ', dd, mm, yy)
        filename = f"{yy}_{mm}_{dd}_raw.dat"
        
        dirname = f"{self.datadir}raw{self.sep}"
        if  not os.path.isdir(dirname):
            os.makedirs(dirname)
        
        filename = f"{dirname}{filename}"
        print(filename)
        with open(filename, "a") as rawfile:
            rawfile.write(self.buff + '\n')
         
         
    ############################################################################
    ## write to ddat data
    ############################################################################
    def parse_format_D_data(self):
        if len(self.buff) < 10:
            return

        if 'ix' in os.name:
            self.buff = self.buff.split("\n")   ## for Linux
        else:
            self.buff = self.buff.split("\r\n") ## for Windows

        ##  --- for excel data
        if self.file_header == '':
            self.fill_header()
        header = self.file_header[self.file_header.find("Date"):].split("; ")
        columns = ['Date(yyyy/MM/dd)', 'Time(hh:mm:ss)', 'BC1', 'BC2', 'BC3', 'BC4', 'BC5', 'BC6', 'BC7', 'BB(%)', "Status"]
        colnums = [header.index(x) for x in columns]

        ##  ---- write from buffer to ddat file --- ##
        filename = ''
        lastline = ''
        need_check = True
        dateformat = "%Y/%m/%d %H:%M:%S"
        ym_patterns = []
        rows_list = []
        for line in self.buff[::-1]:  ##  buff has antichronological order
            #print('line:   ',line)
            yy, mm, _ = line.split()[0].split('/')
            year_month = f"{yy}_{mm}"

            ##  --- for first line or new file
            if year_month not in ym_patterns:
                ym_patterns.append(year_month)
                ##  -- new ddat filename 
                #filename = '_'.join((yy, mm)) + f"_{self.ae_name}.ddat"
                filename = f"{year_month}_{self.ae_name}.ddat"
                if not self.datadir.endswith(self.sep):
                    self.datadir += self.sep
                filename = f"{self.datadir}ddat{self.sep}{filename}"
                try:
                    ## ddat file exists: read last line datetime
                    with open(filename, 'r') as f:
                        lastline = f.readlines()[-1].split()
                        #print(lastline)

                    lasttime = lastline[0] + ' ' + lastline[1]
                    #print('1: ', lasttime)
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

            ##  add line data to dataframe
            line_to_dataframe = [line.split()[i] for i in colnums]
            #print("line_to_dataframe:>",line_to_dataframe)
            line_to_dataframe = line_to_dataframe[:2]\
                                + [int(x) for x in line_to_dataframe[2:-2]]\
                                + [float(line_to_dataframe[-2])]\
                                + [int(line_to_dataframe[-1])]
            rows_list.append(line_to_dataframe)

            ##  check line to be added to datafile
            if need_check: # and len(lastline):
                nowtime = line.split()[0] + ' ' + line.split()[1]
                nowtime = datetime.strptime(nowtime,  dateformat)
                ## if line was printed earlier
                if nowtime <= lasttime:
                    continue

            need_check = False

            ##  write to ddat file
            with open(filename, 'a') as f:
                f.write(line + '\n')
        ##  ---- end of write from buffer to ddat file --- ##
        
        ##  --- read ddat file and write to tables
        ##  get year_month patterns
        print(ym_patterns)
        for year_month in ym_patterns:
            filename = f"{year_month}_{self.ae_name}.ddat"
            filename = f"{self.datadir}ddat{self.sep}{filename}"
            self.convert_one_dat_file_to_table_file(filename)
        
        ##  --- make dataframe_from_buffer
        ##print("\n\n\n!!!!!!!!!!", rows_list)
        dataframe_from_buffer = pd.DataFrame(rows_list, columns=columns)     
        
        ##
        ##  --- Check status errors
        ##
        status = dataframe_from_buffer["Status"][-60:].unique().tolist()
        print("Status:", status)
        errors = [error for i in status for error in parse_errors(i)]       
        errors = sorted(list(set(errors)), 
                        reverse=True, 
                        key=lambda x: int(x.split("(")[1].split(")")[0])
                       )
        if errors:
            errors = "".join(errors)
            self.write_to_bot(errors)
            print("Status:", errors)
   
   
    ############################################################################
    ### 
    ############################################################################
    def convert_one_dat_file_to_table_file(self, filename):     
        df = read_dataframe_from_ddat_file(filename)
        if "s0" in df.columns:
            df = manage_external_devices(df)
        
        ##  --- save to tables
        #self.write_dataframe_to_excel_file(dataframe_from_buffer[self.xlscolumns])
        self.write_dataframe_to_table_files(df)


    ############################################################################
    ### write dataframe to excel file
    ### \return - no return
    ############################################################################
    def write_dataframe_to_table_files(self, dataframe):
        """ write dataframe to table files """
        print("write_dataframe_to_table_files")
        
        ##  extract year and month from data
        year_month = dataframe['Datetime'].apply(select_year_month).unique()

        ##  prepare directory
        table_dirname = self.datadir + 'table' + self.sep
        if not os.path.isdir(table_dirname):
            os.makedirs(table_dirname)

        ##  write to table data file
        for ym_pattern in year_month:
            #print(ym_pattern, end=' ')
            filenamexls = table_dirname + ym_pattern + '_' + self.ae_name + ".xlsx"
            filenamecsv = table_dirname + ym_pattern + '_' + self.ae_name + ".csv"
            self.xlsfilename = filenamexls
            self.csvfilename = filenamecsv

            ## отфильтровать строки за нужный месяц и год
            dfsave = dataframe[dataframe['Datetime'].apply(select_year_month) == ym_pattern]
            text = f"{ym_pattern}: {dfsave.shape}"
            self.print_message(text, '\n')

            print(f"write to {filenamecsv}")
            dfsave.set_index('Datetime').to_csv(filenamecsv, float_format='%g') #.3f')
            print(f"write to {filenamexls}")
            dfsave.set_index('Datetime').to_excel(filenamexls, engine='openpyxl')
            return 0


    ############################################################################
    ### write dataframe to excel file
    ### \return - no return
    ############################################################################
    def write_dataframe_to_excel_file(self, dataframe):
        """ write dataframe to excel file """
        print("write_dataframe_to_data_files")
        
        ##  add columns
        #dataframe.loc[:,'BCbb'] = dataframe.loc[:,'BB(%)'].astype(float) / 100 \
        #                        * dataframe.loc[:,'BC5'].astype(float)
        dataframe.loc[:,['BCbb']] = dataframe.apply(lambda row: 
                                        row['BB(%)'] / 100 * row['BC5'], axis=1)
        dataframe.loc[:,['BCff']] = dataframe.apply(lambda row: 
                                        (100 - row['BB(%)']) / 100 * row['BC5'], axis=1)

        ##  extract year and month from data
        year_month = dataframe['Datetime'].apply(select_year_month).unique()

        ##  prepare directory
        table_dirname = self.datadir + 'table' + self.sep
        if not os.path.isdir(table_dirname):
            os.makedirs(table_dirname)

        ##  write to table data file
        for ym_pattern in year_month:
            #print(ym_pattern, end=' ')
            filenamexls = table_dirname + ym_pattern + '_' + self.ae_name + ".xlsx"
            filenamecsv = table_dirname + ym_pattern + '_' + self.ae_name + ".csv"
            self.xlsfilename = filenamexls
            self.csvfilename = filenamecsv

            ## отфильтровать строки за нужный месяц и год
            dfsave = dataframe[dataframe['Datetime'].apply(select_year_month) == ym_pattern]
            text = f"{ym_pattern}: {dfsave.shape}"
            self.print_message(text, '\n')

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
            dfsave.set_index('Datetime').to_csv(filenamecsv, float_format='%g') #.3f')
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

        ## --- check file header is standart
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
        print(columns)
        #print(len(columns), end=' ')
        print(filename)
        datum = pd.read_csv(filename, sep=r"\s+", on_bad_lines='warn', 
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
        year = 2025 ### \todo Add many years 
        for month in ['04']: ### \todo Add all months
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
def get_columns(filename):
    standart_head = "Date(yyyy/MM/dd); Time(hh:mm:ss); Timebase; RefCh1; Sen1Ch1; Sen2Ch1; RefCh2; Sen1Ch2; Sen2Ch2; RefCh3; Sen1Ch3; Sen2Ch3; RefCh4; Sen1Ch4; Sen2Ch4; RefCh5; Sen1Ch5; Sen2Ch5; RefCh6; Sen1Ch6; Sen2Ch6; RefCh7; Sen1Ch7; Sen2Ch7; Flow1; Flow2; FlowC; Pressure(Pa); Temperature(°C); BB(%); ContTemp; SupplyTemp; Status; ContStatus; DetectStatus; LedStatus; ValveStatus; LedTemp; BC11; BC12; BC1; BC21; BC22; BC2; BC31; BC32; BC3; BC41; BC42; BC4; BC51; BC52; BC5; BC61; BC62; BC6; BC71; BC72; BC7; K1; K2; K3; K4; K5; K6; K7; TapeAdvCount; "
    standart_columns = standart_head.split("; ")[:-1]

    with open(filename, encoding='windows-1252') as f:
        data = f.readlines()
        head = [line for line in data if line.startswith('Date')][0].strip()
        if "Temperature(Â°C)" in head:
            head = head.replace("Temperature(Â°C)", "Temperature(°C)")
        if "BB (%)" in head:
            head = head.replace("BB (%)", "BB(%)")
        #print(head)

    columns = [x.strip() for x in head.split(";")[:-1]]    
    if columns != standart_columns:
        print("Warning! Columns differ from standart columns!!")
        print(f"Extra column in columns: {set(columns) - set(standart_columns)}\n" * bool(set(columns) - set(standart_columns))
            + f"Extra column in standart_columns: {set(standart_columns) - set(columns)}" * bool(set(standart_columns) - set(columns)))
        columns = standart_columns

    numcols = max([len(line.split()) for line in data])
    if len(columns) < numcols:
        columns = columns + [f"s{i}" for i in range(numcols - len(columns))]
        #print(f'new columns: {columns}')
    return  columns
    

    ############################################################################
    ############################################################################
def rows_to_skip(filename):
    """ calculate number of rows to skip"""
    with open(filename, encoding='windows-1252') as f:
        head = [i for i,line in enumerate(f.readlines()) if line.startswith('Date')][0]
        print(f"lines to skip: {head}")
    return head + 1


    ############################################################################
    ############################################################################
def read_dataframe_from_ddat_file(filename):
    """ read ddat or dat file"""
    ##  read data file 
    columns = get_columns(filename)
    df = pd.read_csv(filename, sep=r'\s+', index_col=None, names=columns,
                            skiprows=rows_to_skip(filename), 
                            on_bad_lines='skip', 
                            skip_blank_lines=True, encoding='windows-1252')
    print(df.shape)

    ##  skip bad time lines
    df = df.dropna(subset=["Date(yyyy/MM/dd)", 'Time(hh:mm:ss)'])
    df = df.drop(df[df["Date(yyyy/MM/dd)"].str.startswith('Date')].index)
    #print("dd:", [x for x in list(df["Date(yyyy/MM/dd)"]) if '/' not in str(x)])
    #print("tt:", [x for x in list(df['Time(hh:mm:ss)']) if ':' not in str(x)])
    df = df[df["Date(yyyy/MM/dd)"].str.contains('/')]
    df = df[df['Time(hh:mm:ss)'].str.contains(':')]
    print(df.shape)

    ##  reformat and add columns
    if "BB (%)" in df.columns:
        df = df.rename(columns={"BB (%)": "BB(%)"})
    for com in ['BC1', 'BC2', 'BC3', 'BC4', 'BC5', 'BC6', 'BC7']:
        df[com] = df[com].astype(int)
    df.insert(0, "Datetime", df.apply(lambda row: ".".join(row['Date(yyyy/MM/dd)'].split('/')[::-1]) + ' '
                                                + ':'.join(row['Time(hh:mm:ss)'].split(':')[:2]), axis=1))

    ##  select columns to table
    table_columns  = ['Datetime', 'BC1', 'BC2', 'BC3', 'BC4', 'BC5', 'BC6', 'BC7', 'BB(%)', "Status"]
    table_columns += [column for column in df.columns if column.startswith("s")]
    df = df[table_columns]

    ##  add timestamp column
    df["timestamp"] = df.apply(lambda row: int(datetime.strptime(row["Datetime"], "%d.%m.%Y %H:%M").timestamp()), axis=1) ## 01.01.2025 00:00                                               
    ##  add BCbb and BCff columns
    bbcol = [col for col in df.columns if ('BB' in col) and ('%' in col)][0] ##  'BB(%)' or 'BB (%)'
    df[bbcol] = df[bbcol].astype(float)
    df.loc[:,['BCbb']] = df.apply(lambda row:        row[bbcol]  / 100 * row['BC5'], axis=1)
    df.loc[:,['BCff']] = df.apply(lambda row: (100 - row[bbcol]) / 100 * row['BC5'], axis=1)

    print(df.shape)
    return df


    ############################################################################
    ############################################################################
def manage_external_devices(df):
    external_devices = { 
        ##  [name, description, Data line]
        3:  ["Vaisala_GMP343", "CO2 sensor", 
            "CO2 [ppm], CO2RAW [ppm], T [°C]"], 
        #5:  ["Datalogger_AE33_protocol", "the port is enabled for communication with the data-logger", ""],
        6:  ["Aerosol_inlet_dryer", "Sample stream dryer - temperature, RH, dew point sensor", 
            "Tin [°C], RHin [%], Td-in [°C], Tout [°C], RHout [%], Td-out [°C]"],
        17: ["Gill_GMX200", "Ultrasonic wind speed & direction sensor", 
            "wind speed [m/s], wind direction [°], wind direction corrected [°], status17"],
        18: ["Gill_GMX300", "Temperature, Humidity & Pressure Sensor", 
            "T [°C], RH [%], P [bar], Td [°C], status18"],
        19: ["Gill_GMX500", "Compact weather station - temperature, humidity & pressure and wind speed and direction", 
            "T [°C], RH [%], P [bar], Td [°C], wind speed [m/s], wind direction [°], wind direction corrected [°], status19"]
        }

    ##  extract devices
    devices = [df[com].dropna().unique().tolist() for com in ["s0", "s1", "s2"]]
    devices = [int(item) for sublist in devices for item in sublist if item!=0]
    print(f"devices: {devices}")

    ##  extract columns for external devices
    external_columns = [external_devices[device][2].split(", ") for device in devices if (device in external_devices) and (device not in [0, 5])]        
    external_columns = [item for sublist in external_columns for item in sublist]
    print(f"external_columns: {external_columns}")

    ##  find extra columns in df
    df_external_columns = [col for col in df.columns if col.startswith("s") and col not in ["s0", "s1", "s2"]]
    #print(df_external_columns)

    ##  merge and rename columns
    if len(df_external_columns) < len(external_columns):
        print(f"Error! No enough columns to read external data. {len(df_external_columns)} < {len(external_columns)}")
    elif len(df_external_columns) == len(external_columns):
        merge_dirt = {old:new for old,new in zip(df_external_columns, external_columns)}
        df = df.rename(columns=merge_dirt)
    
    return df



############################################################################
############################################################################
##  Parse status errors
def parse_errors(error):
    errors = []

    ## Статус эксплуатации
    if error & 1 and error & 2:
        errors.append(f"Error Status(3). Остановка. Прибор не работает!\n") 
    elif error & 1:
        errors.append(f"Status(1). Протягивание ленты (обычное продвижение ленты, быстрая калибровка, прогрев).\n")
    elif error & 2:
        errors.append(f"Status(2). Первое измерение – получение ATN0.\n")
    
    ## Статус расход
    if error & 4 and error & 8:
        errors.append(f"Error Status(12). Расход низкий/высокий и историю состояния расхода.\n")
    elif error & 4:
        errors.append(f"Error Status(4). Расход меньше/выше, чем на 0.5 л/мин или F1<0 или отношение F2/F1 за пределом диапазона 0.2 – 0.75.\n")
    elif error & 8:
        errors.append(f"Error Status(8). Проверьте историю состояния расхода.\n")

    ## Статус Источник излучения
    if error & 16 and error & 32:
        errors.append(f"Error Status(48). Сбой в работе светодиодов (сбой по всем каналам).\n")
    elif error & 16:
        errors.append(f"Error Status(16). Калибровка светодиодов.\n")
    elif error & 32:
        errors.append(f"Error Status(32). Сбой калибровки (по крайней мере один из каналов в норме).\n")

    ## Статус Измерительная камера
    if error & 64:
        errors.append(f"Error Status(64). Сбой в измерительной камере.\n")

    ## Статус Фильтрующая лента
    if error & 128 and error & 256:
        errors.append(f"Error Status(384). Сбой ленты (лента не движется, лента закончилась).\n")
    elif error & 128:
        ##  писать предупреждение только при протягивании ленты
        if error & 1 and not (error & 2):
            errors.append(f"Status(128). Предупреждение (осталось менее 30 спотов).\n")
    elif error & 256:
        errors.append(f"Error Status(256). Последнее предупреждение (осталось менее 5 спотов).\n")

    ## Установочный файл
    if error & 512:
        errors.append(f"Error Status(512). Предупреждение по установочному файлу.\n")

    ## Испытания и процедуры 10, 11, 12 биты
    if error & 2048 and error & 1024:
        errors.append(f"Error Status(3072). Процедура замены ленты.\n")
    elif error & 2048 and error & 4096:
        errors.append(f"Error Status(6144). Тест на утечки.\n")
    elif error & 1024:
        errors.append(f"Error Status(1024). Испытание на устойчивость.\n")
    elif error & 2048:
        errors.append(f"Error Status(2048). Продувка чистым воздухом.\n")
    elif error & 4096:
        errors.append(f"Error Status(4096). Испытание оптической системы.\n")

    ## Внешние устройства
    if error & 8192:
        errors.append(f"Error Status(8192). Сбой в соединении внешних устройств.\n")

    ## Авто-испытание «нуль»-воздуха
    if error & 16384:
        errors.append(f"Error Status(16384). Результат проверки «нуль-воздуха» неприемлем; рекомендуется сервисное обслуживание прибора.\n")
    ## Сбой карты
    if error & 32768:
        errors.append(f"Error Status(32768). Сбой при сохранении или восстановлении файлов при работе с CF-картой.\n")
    
    ## Состояние базы данных
    if error >= 65535:
        errors.append(f"Error Status(65535). Размер базы данных превышает 2*106 строк.\n")

    ## Вернуть ошибки
    if errors:
        return errors
    return []


## print(parse_errors(879706))