import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from   matplotlib import dates
import os


############################################################################
## make resample by three close measurements
############################################################################ 
def average_by_three(datum):
    if 'Date' in datum.columns:
        datum['dt'] = datum['Date'] + ' ' + datum['Time (Moscow)']
        fmt = '%Y/%m/%d %H:%M:%S'
    else:
        datum['dt'] = datum['Datetime']
        fmt = '%d.%m.%Y %H:%M'

    datum.set_index('dt', inplace=True)
    datum.index = pd.to_datetime(datum.index, format=fmt) # format='%m/%d%Y %-I%M%S %p'

    return datum.resample("3T").mean().fillna(0).rolling(window=3, min_periods=1).mean()
    #return datum.resample("3T").sum().rolling(window=3, min_periods=1).mean()


############################################################################
##  Return possible datatime format
############################################################################
def get_time_format():
    ##  check if format is possible
    fmt = dates.DateFormatter('%d-%2m-%Y\n %H:%M')
    try:
        print(datetime.now().strftime(fmt))
    except:
        fmt = dates.DateFormatter('%d/%m/%Y\n %H:%M')
    return fmt


############################################################################
##  Prepare data to plot graphs
############################################################################
def get_year_from_filename(name):
    if 'ix' in os.name:
        sep = '/'  ## -- path separator for LINIX
    else:
        sep = '\\' ## -- path separator for Windows

    year = name.split(sep)[-1].split('_')[0]
    month = name.split(sep)[-1].split('_')[1]
    return int(year), int(month)


############################################################################
##  Get data from previous_month
############################################################################
def get_data_from_previous_month(name):
    #sep = '/'
    if 'ix' in os.name:
        sep = '/'  ## -- path separator for LINIX
    else:
        sep = '\\' ## -- path separator for Windows

    ##  get actual year and month
    year, month = get_year_from_filename(name)
    #print(year, month)
    ##  calculate previous year and month
    newmonth =   12    if month == 1 else month - 1 
    newyear = year - 1 if month == 1 else year
    print(newyear, newmonth)

    ##  replace year and month in filename
    print(name)
    nparts = name.split(sep)
    nfile = nparts[-1].split('_')
    nfile[0] = str(newyear)
    nfile[1] = f'{newmonth:02d}'
    nparts[-1] = '_'.join(nfile)
    newname = sep.join(nparts)
    print(newname)

    ## get previous month data
    data = pd.read_excel(newname)

    return data



############################################################################
##  Prepare data to plot graphs
##  get 2 week data from data files
############################################################################
def prepare_data(xlsfilename):
    data = pd.read_excel(xlsfilename)

    ##  make column to plot on x axis
    if 'Date' in data.columns:
        x = (data['Date'].astype('string') + ' ' + data['Time (Moscow)'].astype('string'))
        x = pd.to_datetime(x, format='%Y/%m/%d %H:%M:%S')
    else:
        x = pd.to_datetime(data['Datetime'], format='%d.%m.%Y %H:%M')
    #print(x)

    ## если данных меньше, чем 2 недели, считать данные за прошлый месяц
    if x.min() + pd.to_timedelta("336:00:00") > x.max():
        print("Data file has less than 2 week data")
        olddata = get_data_from_previous_month(xlsfilename)
        data = pd.concat([olddata, data], ignore_index=True)
        #print(f"joined data: {data.shape}\n", data.head())
        ## make column to plot on x axis
        if 'Date' in data.columns:
            x = (data['Date'].astype('string') + ' ' + data['Time (Moscow)'].astype('string'))
            x = pd.to_datetime(x, format='%Y/%m/%d %H:%M:%S')
        else:
            x = pd.to_datetime(data['Datetime'], format='%d.%m.%Y %H:%M')
    else:
        print("One file is enouth")

    data['plotx'] = x

    ##  оставить только две недели
    xmin = x.max() - pd.to_timedelta("336:00:00") ## 14 days
    #print("xmin: ", xmin)
    data = data[pd.to_datetime(data['plotx']) > xmin]
    #print(f"only 2 weeks: {data.shape}\n", data.head())

    return data



############################################################################
## Create plots from excel file with Aethalometer data
#  @param nfigs - number of files to create
############################################################################
##def plot_four_figures_from_excel(self, xlsfilename)
def plot_four_figures_from_excel(xlsfilename, path_to_figures, nfigs=1):
    print(f"Plot  {nfigs}  figures")
    #print(xlsfilename, path_to_figures, nfigs)

    #print(path_to_figures)
    if not os.path.isdir(path_to_figures):
        os.makedirs(path_to_figures)


    ## read and prepare data
    datum = prepare_data(xlsfilename)
    print(datum.head(2))


    ## get 2 days data
    xmin = datum.plotx.max() - pd.to_timedelta("48:01:00")  ##  2 days
    print("xmin: ", xmin)
    data = datum[datum.plotx >= xmin]
    x = data['plotx']

    # format graph
    fmt = get_time_format()
    plt.rcParams['xtick.labelsize'] = 10
    facecolor = 'white'
    title = xlsfilename.split('/')[-1].split('/')[-1].split('.')[-2].split('_')[-1]
    #title = ae_name
    xlims = (x.min(), x.max() + pd.to_timedelta("2:00:00"))


    ##########################
    ## Figure1: 7 waves
    if nfigs == 1:
        fig = plt.figure(figsize=(16, 12))
        ax_1 = fig.add_subplot(3, 1, 1)
    else:
        fig = plt.figure(figsize=(10, 5))
        ax_1 = fig.add_subplot(1, 1, 1)

    for i in range(7):
        wave = 'BC' + str(i + 1)
        y = data[wave].replace(0, np.nan)
        if i == 0:
            ax_1.plot(x, y, color='red', label=wave)
        elif i == 5:
            ax_1.plot(x, y, color='black', label=wave)
        else:
            ax_1.plot(x, y, label=wave)
    ax_1.xaxis.set_major_formatter(fmt)
    ax_1.set_xlim(xlims)
    ax_1.set_ylim(bottom=0)
    ax_1.legend()
    ax_1.set_title(title, loc='right')
    ax_1.grid()
    ## save to file "ae33_bc_waves.png"
    if nfigs != 1:
        fig.savefig(path_to_figures + 'ae33_bc_waves_day.svg', facecolor=facecolor, bbox_inches='tight') 
        fig.savefig(path_to_figures + 'ae33_bc_waves_day.png', facecolor=facecolor, bbox_inches='tight') 


    ##########################
    ## Figure 2: BCff, BCbb
    if nfigs == 1:
        ax_2 = fig.add_subplot(3, 1, 2)
    else:
        fig = plt.figure(figsize=(10, 5))
        ax_2 = fig.add_subplot(1, 1, 1)

    ax_2.plot(x, data["BCff"].replace(0, np.nan), 'k', label='BCff')
    ax_2.fill_between(x, data["BCff"].replace(0, np.nan), np.zeros_like(y), color='black')
    ax_2.plot(x, data["BCbb"].replace(0, np.nan), 'orange', label='BCbb')
    ax_2.fill_between(x, data["BCbb"].replace(0, np.nan), np.zeros_like(y), color='orange')
    ax_2.xaxis.set_major_formatter(fmt)
    ax_2.set_xlim(xlims)
    ax_2.set_ylim(bottom=0)
    ax_2.legend()
    ax_2.grid()
    ## save to file "ae33_bc.png"
    if nfigs != 1:
        ax_2.set_title(title, loc='right')
        fig.savefig(path_to_figures + 'ae33_bc_day.svg', facecolor=facecolor, bbox_inches='tight') 
        fig.savefig(path_to_figures + 'ae33_bc_day.png', facecolor=facecolor, bbox_inches='tight') 


    #####################################
    #####################################
    ## Make average by three points
    data = average_by_three(datum)
    print("+++++>\n", datum.head(10))
    print("=====>\n", data.head(10))
    ## get only last two weeks
    xmin = data.index.max() - pd.to_timedelta("336:00:00") ## 14 days
    data = data[data.index >= xmin]

    ## set new axis label format
    fmt = dates.DateFormatter('%d-%2m-%Y')
    try:
        print(datetime.now().strftime(fmt))
    except:
        fmt = dates.DateFormatter('%d/%m/%Y')

    plt.rcParams['xtick.labelsize'] = 8
    print(data.index.min(), data.index.max(), "delta:", data.index.max() - data.index.min())

    ##########################
    ## Figure 3: 
    if nfigs == 1:
        ax_3 = fig.add_subplot(3, 2, 5)
        #ax_3 = fig.add_subplot(4, 2, 7)
    else:
        fig = plt.figure(figsize=(8, 4))
        ax_3 = fig.add_subplot(1, 1, 1)

    for i in range(7):
        wave = 'BC' + str(i + 1)
        xx = data[wave].replace(0, np.nan)

        if i == 0:
            ax_3.plot(xx, color='red',   label=wave)
        elif i == 5:
            ax_3.plot(xx, color='black', label=wave)
        else:
            ax_3.plot(xx, label=wave)

    ax_3.xaxis.set_major_formatter(fmt)
    #ax_3.set_xlim(left=xx.index.min())
    ax_3.set_xlim(left=xmin)
    ax_3.set_ylim(bottom=0)
    ax_3.legend() # ncol = 7, fontsize = 9)
    ax_3.grid()
    ## save to file "ae33_bc_waves_week.png"
    if nfigs != 1:
        ax_3.set_title(title, loc='right')
        fig.savefig(path_to_figures + 'ae33_bc_waves_week.png', facecolor=facecolor) 


    ##########################
    ## Figure 4: BCff BCbb
    if nfigs == 1:
        ax_4 = fig.add_subplot(3, 2, 6)
    else:
        fig = plt.figure(figsize=(8, 4))
        ax_4 = fig.add_subplot(1, 1, 1)

    yy = data["BCff"].replace(0, np.nan)
    zz = data["BCbb"].replace(0, np.nan)
    ax_4.plot(yy, 'k', label='BCff')
    ax_4.fill_between(yy.index, yy, np.zeros_like(yy), color='black')
    ax_4.plot(zz, 'orange', label='BCbb')
    ax_4.fill_between(zz.index, zz, np.zeros_like(zz), color='orange')
    ax_4.xaxis.set_major_formatter(fmt)
    #ax_4.set_xlim(left=zz.index.min())
    ax_4.set_xlim(left=xmin)
    ax_4.set_ylim(bottom=0)
    ax_4.legend()
    ax_4.grid()

    ## save one figure to file "ae33_bc_week.png"
    if nfigs != 1:
        ax_4.set_title(title, loc='right')
        fig.savefig(path_to_figures + 'ae33_bc_week.png', facecolor=facecolor) 


    #####################################
    ## save four plots to file
    if nfigs == 1:
        filename = path_to_figures + 'ae33_bc_four_plots.png'
        print(filename)
        fig.savefig(filename, 
                    facecolor=facecolor, # facecolor='lightgray',
                    #, bbox_inches = 'tight'
                   )



## --------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    path_to_figures = "./figures/"
    ae_name = 'AE33-S08-01006'
    #ae_name = 'AE33-S09-01249'
    dataname = "_".join(ae_name.split('-'))
    dirname = './data/' + dataname + '/table/'
    timestamp = '2022_04'  #'2022_06'
    filename = timestamp + '_' + ae_name + '.xlsx'

    # create one figure with four graphs
    #plot_four_figures_from_excel('./data/table/2022_03_AE33-S08-01006.xlsx', path_to_figures )
    plot_four_figures_from_excel(dirname + filename, path_to_figures)

    # create four figures
    #plot_four_figures_from_excel('./data/table/'+ timestamp + '_' + ae_name + '.xlsx', path_to_figures, 4 )
    plot_four_figures_from_excel(dirname + filename, path_to_figures, 4)
