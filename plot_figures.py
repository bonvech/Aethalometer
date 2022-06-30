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

    #return datum.resample("3T").sum().fillna().rolling(window=3, min_periods=1).mean()
    return datum.resample("3T").sum().rolling(window=3, min_periods=1).mean()


############################################################################
## prepare data to plot
############################################################################ 
def prepare_data(datum):
    ## remove nulls from data
    for x in datum.columns:
        if "atetime" in x: continue
        #datum[x][datum[x] == 0] = np.nan
        mask = datum[x] == 0
        #datum.loc[mask, x] = np.nan
        #datum.loc[:, x] = datum[x].replace(0, np.nan)
        #print(x)

    return datum



############################################################################
## Return possible datatime format
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
## Create plots from excel file with Aethalometer data
#  @param nfigs - number of files to create
############################################################################
##def plot_four_figures_from_excel(self, xlsfilename)
def plot_four_figures_from_excel(xlsfilename, path_to_figures, nfigs=1):
    print(f"Plot  {nfigs}  figures")

    #print(path_to_figures)
    if not os.path.isdir(path_to_figures):
        os.makedirs(path_to_figures)


    ## read data
    datum = pd.read_excel(xlsfilename)
    ## take two days data
    data = datum[-2880:]
    data = prepare_data(data)
    if 'Date' in data.columns:
        x = (data['Date'].astype('string') + ' ' + data['Time (Moscow)'].astype('string'))
        x = pd.to_datetime(x, format='%Y/%m/%d %H:%M:%S')
    else:
        x = data['Datetime'].astype('string')
        x = pd.to_datetime(x, format='%d.%m.%Y %H:%M')
    #print(x)

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
        fig.savefig(path_to_figures + 'ae33_bc_day.svg', 
                    facecolor=facecolor, bbox_inches='tight') 
        fig.savefig(path_to_figures + 'ae33_bc_day.png', 
                    facecolor=facecolor, bbox_inches='tight') 


    #####################################
    #####################################
    ## Make average by three points
    data = average_by_three(datum)
    ## get only last two weeks
    xmin = data.index.max() - pd.to_timedelta("336:00:00")
    data = data[data.index > xmin]

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
        ax_3.plot(xx, label=wave)
    ax_3.xaxis.set_major_formatter(fmt)
    ax_3.set_xlim(left=xx.index.min())
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
    ax_4.set_xlim(left=zz.index.min())
    ax_4.set_ylim(bottom=0)
    ax_4.legend()
    ax_4.grid()

    ## save one figure to file "ae33_bc_week.png"
    if nfigs != 1:
        ax_4.set_title(title, loc='right')
        fig.savefig(path_to_figures + 'ae33_bc_week.png', facecolor=facecolor) 


    #####################################
    ## save four plots to file
    #fig.savefig('ae33_four_plots.png', facecolor='lightgray') # bbox_inches = 'tight'
    if nfigs == 1:
        print(path_to_figures + 'ae33_bc_four_plots.png')
        fig.savefig(path_to_figures + 'ae33_bc_four_plots.png', 
                    facecolor=facecolor,
                    #datetime_format='%d/%m/%Y'
                    #, bbox_inches = 'tight'
                    )



## --------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    path_to_figures = "./figures/"
    ae_name = 'AE33-S09-01249'
    timestamp = '2022_06'

    # create one figure with four graphs
    #plot_four_figures_from_excel('./data/table/2022_03_AE33-S08-01006.xlsx', path_to_figures )
    plot_four_figures_from_excel('./data/table/'+ timestamp + '_' + ae_name + '.xlsx', path_to_figures )

    # create four figures
    #plot_four_figures_from_excel('./data/table/2022_03_AE33-S08-01006.xlsx', path_to_figures, 4)
    plot_four_figures_from_excel('./data/table/'+ timestamp + '_' + ae_name + '.xlsx', path_to_figures, 4 )
