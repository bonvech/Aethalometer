import sys
import socket
import time
import os


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
        self.file_raw = None       ## file for raw data
        self.file_format_D = None  ## file for raw data
        self.file_header = ''

        self.run_mode = 0

        self.buff = ''
        self.info = ''
        self.IPname = '213.131.1.46'
        self.IsConnected = 1
        self.Port = 8002  ## port number
        self.sock = None  ## socket

        self.fill_header()


    def fill_header(self):
        self.file_header = "AETHALOMETER\nSerial number = AE33-S08-01006\nApplication version = 1.6.7.0\nNumber of channels = 7\n\nDate(yyyy/MM/dd); Time(hh:mm:ss); Timebase; RefCh1; Sen1Ch1; Sen2Ch1; RefCh2; Sen1Ch2; Sen2Ch2; RefCh3; Sen1Ch3; Sen2Ch3; RefCh4; Sen1Ch4; Sen2Ch4; RefCh5; Sen1Ch5; Sen2Ch5; RefCh6; Sen1Ch6; Sen2Ch6; RefCh7; Sen1Ch7; Sen2Ch7; Flow1; Flow2; FlowC; Pressure(Pa); Temperature(°C); BB(%); ContTemp; SupplyTemp; Status; ContStatus; DetectStatus; LedStatus; ValveStatus; LedTemp; BC11; BC12; BC1; BC21; BC22; BC2; BC31; BC32; BC3; BC41; BC42; BC4; BC51; BC52; BC5; BC61; BC62; BC6; BC71; BC72; BC7; K1; K2; K3; K4; K5; K6; K7; TapeAdvCount;\n\n\n"


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
                os.system("mkdir " + param)
    # \todo ПОПРАВИТЬ в конфигурацилонном файле СЛЕШИ В ИМЕНИ ДИРЕКТОРИИ  !!!   для ВИНДА


    def write_path_file(self):
        f = open("PATHFILES.CNF.bak", 'w')
        f.write("#\n")
        f.write("# Programm mode:\n")
        f.write("#   1 - for MAIN-menu,  0 - Auto RUN\n")
        f.write("#\n")
        f.write("RUN=" + str(self.run_mode) + "\n")
        f.write("#\n")
        f.write("# Directory for DATA:\n")
        f.write("#\n")
        f.write(self.pathfile + '\n')
        f.write("#\n")
        f.write("# AE33:   IP address and Port:\n")
        f.write("#\n")
        f.write("IP=" + self.IPname + '  ' + str(self.Port) +"\n")
        f.write("#\n")
        f.write("# AE33:  Last Records:\n")
        f.write("#\n")
        f.write("MINID=" + str(self.MAXID) + "\n")
        f.write("#\n")
        f.close()


    def print_params(self):
        print("RUN = ", self.run_mode)
        print("IP = ", self.IPname)
        print("Port = ", self.Port)
        print("pathfile = ", self.pathfile)
        print("MINID = ", self.MINID)


    def connect(self):
        if self.active == -1:
            return -1
        
        #socket.socket(family='AF_INET', type='SOCK_STREAM', proto=0, fileno=None)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM | socket.SOCK_NONBLOCK);
        self.sock.connect((self.IPname, self.Port)) 
        ## \todo проверить, что связь установлена


    def unconnect(self):
        ##  socket.shutdown(self.sock, SHUT_RD|SHUT_WR)
        self.sock.close()


    def request(self, command, start, stop):
        if command == 'FETCH DATA':
            command += ' ' + str(start) + ' ' + str(stop)
        command += '\r\n'

        ## --- send command ---
        time.sleep(1)
        ##self.sock.send(bytes(command))
        bytes = self.sock.send(command.encode())
        ## \todo проверить, что все отправилось
        if bytes != len(command):
            print("request: Error in sending data!! ") 
        print('sent ', bytes, ' to socket')

        if "CLOSE" in command:
            return 1

        ## --- read data ---
        time.sleep(1)
        attempts = 0
        self.buff = self.sock.recv()
        while(len(buff) == 0 and attempts < 10):
            time.sleep(1)
            self.buff = self.sock.recv()
            attempts += 1
        if attempts >= 10:
            print("request: Error in receive")
            self.sock.unconnect()
            return 2
        
        if "MAXID" in command:
            self.MAXID = int(buff)
        if "MINID" in command:
            self.MINID = int(buff)
        if '?' in command:
            self.info = buff
        if "FETCH" in command:
            self.parse_raw_data()
        if "AE33" in command:
            if "AE33:D":
                self.parse_format_D_data()            
        return 0


    def parse_raw_data(self):
        if len(self.buff) < 10:
            return
        self.buff = self.buff.split("AE33>")
        for line in self.buff:
            if len(line) < 50:
                continue

            mm, dd, yy = line.split("|")[2][:10].split('/')
            if mm != self.mm or yy != self.yy:
                filename = '_'.join((yy, mm)) + '_AE33-S08-01006.raw'
                filename = self.pathfile + filename
                if self.file_raw:
                    self.file_raw.close()
                self.file_raw = open(filename, "a")
            self.file_raw.write(line)
            self.file_raw.write('\n')
            
        self.file_raw.flush()
        self.mm = mm
        self.yy = yy


    def parse_format_D_data(self):
        ## for test 
        #f = open("buffer.txt")
        #self.buff = f.read().split('\n')
        #f.close()
        #print(self.buff)

        ## main
        if len(self.buff) < 10:
            return

        lastmm, lastyy = '0', '0'
        filename = ''
        lastline = ''
        need_check = True
        dateformat = "%Y/%m/%d %H:%M:%S"
        for line in self.buff[::-1]:
            print(line)
            yy, mm, _ = line.split()[0].split('/')
            #print(yy, mm)

            # for first line or new file
            if mm != lastmm or yy != lastyy:
                filename = '_'.join((yy, mm)) + '_AE33-S08-01006.ddat'
                filename = self.pathfile + filename
                try:
                    ## file exists
                    f = open(filename, 'r')
                    lastline = f.readlines()[-1].split()
                    lasttime = lastline[0] + ' ' + lastline[1]
                    lasttime = datetime.strptime(lasttime, dateformat)
                    f.close()
                    lastmm = mm
                    lastyy = yy
                    need_check = True
                except:
                    ## no file
                    f = open(filename, 'a')        
                    f.write(self.file_header)
                    f.close()
                    lastline = []
                    need_check = False

            ## check line
            if need_check: # and len(lastline):
                print(line)
                nowtime  = line.split()[0] + ' ' + line.split()[1]
                print(nowtime)
                nowtime  = datetime.strptime(nowtime,  dateformat)
                print(nowtime - lasttime)
                if nowtime <= lasttime:
                    continue

            need_check = False

            ## write to file
            f = open(filename, 'a')
            f.write(line)
            f.write('\n')
            f.close()
