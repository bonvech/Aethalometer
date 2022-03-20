from AE33_device import AE33_device
from plot_figures import *
import os


### Read every day files AE33
ae_name = 'AE33-S09-01249'
ae_name = 'AE33-S08-01006'
dirname = './data/' + ae_name.replace('-', '_') + '/' ##< dir to read data from
if not os.path.isdir(dirname):
    print("No directory to read data exists: ", dirname)
    exit()

device = AE33_device()
device.ae_name = ae_name

## --- read data
data = device.read_every_day_files(dirname)
print("data: ", data.shape)

## --- write data
device.write_dataframe_to_excel_file(data)

#x = input("Press ENTER for finish")
