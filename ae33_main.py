from AE33_device import AE33_device
from plot_figures import *


device = AE33_device()

device.read_path_file()
device.print_params()

if device.connect() == 1:
    text = "Connect error"
    device.print_message(text, '\n')
    exit("Connect error")

device.request('HELLO',0,0)
#device.request('?',0,0)  #- не работает
device.request('MAXID DATA',0,0)  #- не работает

##   1 измерений -->  403  bytes
##   1000 измерений -->  48018  bytes
##   за сутки  1440  измеений  ~  600 000 bytes
##   за месяц  43200  измерений  ~  20 000 000 bytes

delay = 100
start = device.MAXID - delay
fin = device.MAXID
print(start, fin)

#device.request('FETCH DATA',start,fin)  #-  не работает, перевести байты в стринги
device.request('$AE33:D' + str(delay), 0, 0)  #-  то же самое
device.request('CLOSE', 0, 0)
device.unconnect()
device.write_path_file()


###############################################################
##  plot figures
##
path_to_figures = "." + device.sep + "figures" + device.sep
# create one figure with four graphs
plot_four_figures_from_excel(device.xlsfilename, path_to_figures, )
# create four figures
plot_four_figures_from_excel(device.xlsfilename, path_to_figures, 4)

