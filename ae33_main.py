from ae33_device import AE33_device
from ae33_plot_figures import *

try:
    device = AE33_device()

    ##  start connection
    if device.connect() == 1:
        text = "Connect error"
        device.print_message(text, '\n')
        exit("Connect error")

    ##  get device name
    print("\n========== request('HELLO') =========================")
    device.request('HELLO',0,0)
    
    print("\n========== request('MAXID DATA') =========================")
    #device.request('?',0,0)  #- не работает
    device.request('MAXID DATA',0,0)  #- не работает

    ##                1 измерений  ->        403 bytes
    ##             1000 измерений  ->    480 180 bytes
    ##  за сутки   1440 измерений  ~     600 000 bytes
    ##  за месяц  43200 измерений  ~  20 000 000 bytes


    ##  request for data
    print("\n========== request('$AE33:D') =========================")
    delay = 100
    device.request('$AE33:D' + str(delay), 0, 0)  ##
   
    ##  get device name
    print("\n=== request('A' ) Запрос на оставшееся количество ленты =======")
    device.request('$AE33:A',0,0)

    ##  close connection
    print("\n==========  close connection =========================")
    device.request('CLOSE', 0, 0)
    device.unconnect()

    ##  save config
    #device.write_config_file()
except Exception as error:
    device.write_to_bot(f"{device.device_name}: Final Error in main programm: {error}. The programm stopped. Start new programm now!")



###############################################################
##  plot figures
##
print("\n==========  plot =========================")
path_to_figures = "." + device.sep + "figures" + device.sep

##  create one figure with four graphs
plot_four_figures_from_csv(device.csvfilename, path_to_figures, )

##  create four figures
plot_four_figures_from_csv(device.csvfilename, path_to_figures, 4)


###############################################################
##
ddat_dirname = device.datadir + 'ddat' + device.sep
#device.read_every_month_files(ddat_dirname)

filename = ddat_dirname + "2025_04_" + device.ae_name + ".ddat"
#device.read_ddat_file(filename)
