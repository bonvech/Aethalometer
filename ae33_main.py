from ae33_device import AE33_device
from ae33_plot_figures import *


device = AE33_device()

##  start connection
if device.connect() == 1:
    text = "Connect error"
    device.print_message(text, '\n')
    exit("Connect error")

device.request('HELLO',0,0)

print("\n========== request('MAXID DATA') =========================")
#device.request('?',0,0)  #- не работает
device.request('MAXID DATA',0,0)  #- не работает

##   1 измерений -->  403  bytes
##   1000 измерений -->  48018  bytes
##   за сутки  1440  измеений  ~  600 000 bytes
##   за месяц  43200  измерений  ~  20 000 000 bytes

print("\n========== request('$AE33:D') =========================")
##  request for data
delay = 100
#start = device.MAXID - delay
#fin = device.MAXID
#print(start, fin)
#device.request('FETCH DATA',start,fin)  #-  не работает, перевести байты в стринги
device.request('$AE33:D' + str(delay), 0, 0)  #-  то же самое

##  close connection
print("\n==========  close connection =========================")
device.request('CLOSE', 0, 0)
device.unconnect()

##  save config
device.write_config_file()



###############################################################
##  plot figures
##
print("\n==========  plot =========================")
path_to_figures = "." + device.sep + "figures" + device.sep

# create one figure with four graphs
plot_four_figures_from_csv(device.csvfilename, path_to_figures, )

# create four figures
plot_four_figures_from_csv(device.csvfilename, path_to_figures, 4)

