import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import dates


############################################################################
## make resample by three close measurements
############################################################################ 
def average_by_three(datum):
    datum['dt'] = datum['Date'] + ' ' + datum['Time (Moscow)']
    datum.set_index('dt',inplace=True)
    datum.index = pd.to_datetime(datum.index, format='%Y/%m/%d %H:%M:%S') # format='%m/%d%Y %-I%M%S %p'
    return datum.resample("3T").sum().fillna(0).rolling(window=3, min_periods=1).mean()


############################################################################
## Create plots from excel file with Aethalometer data
#  @param nfigs - number of files to create
############################################################################
##def plot_four_figures_from_excel(self, xlsfilename)
def plot_four_figures_from_excel(xlsfilename, path_to_figures, nfigs=1):
    #print(path_to_figures)
    #fmt="%-m/%-d/%Y"
    fmt = dates.DateFormatter('%d-%2m-%Y\n %H:%M')
    try:
        print(datetime.now().strftime(fmt))
    except:
        #print('fmt="%m/%d/%Y"')
        fmt = dates.DateFormatter('%d/%m/%Y\n %H:%M')
    
    
    ## read data
    datum = pd.read_excel(xlsfilename)
    ## take two days data
    data = datum[-2880:]
    x = (data['Date'].astype('string') + ' ' + data['Time (Moscow)'].astype('string')).map(pd.to_datetime)
    xlims = (x.min(), x.max() + pd.to_timedelta("2:00:00"))

    # format graph
    plt.rcParams['xtick.labelsize'] = 10
    facecolor = 'white'


    ##########################
    ## Figure1: 7 waves
    if nfigs == 1:
        fig = plt.figure(figsize=(16, 12))
        ax_1 = fig.add_subplot(3, 1, 1)
    else:
        fig = plt.figure(figsize=(16, 4))
        ax_1 = fig.add_subplot(1, 1, 1)

    for i in range(7):
        wave = 'BC' + str(i + 1)
        ax_1.plot(x, data[wave], label=wave)
    ax_1.xaxis.set_major_formatter(fmt)
    ax_1.set_xlim(xlims)
    ax_1.legend()
    ax_1.grid()
    ## save to file "ae33_bc_waves.png"
    if nfigs != 1:
        fig.savefig(path_to_figures + 'ae33_bc_waves_day.png', facecolor=facecolor) 


    ##########################
    ## Figure 2: BCff, BCbb
    if nfigs == 1:
        ax_2 = fig.add_subplot(3, 1, 2)
    else:
        fig = plt.figure(figsize=(16, 4))
        ax_2 = fig.add_subplot(1, 1, 1)
    
    ax_2.plot(x, data["BCff"], 'k', label='BCff')
    ax_2.plot(x, data["BCbb"], 'orange', label='BCbb')
    ax_2.xaxis.set_major_formatter(fmt)
    ax_2.set_xlim(xlims)
    ax_2.legend()
    ax_2.grid()
    ## save to file "ae33_bc.png"
    if nfigs != 1:
        fig.savefig(path_to_figures + 'ae33_bc_day.png', facecolor=facecolor) 


    #####################################
    ## Make average by three points
    data = average_by_three(datum)
    yy = data["BCff"]

    fmt = dates.DateFormatter('%d-%2m-%Y')
    try:
        print(datetime.now().strftime(fmt))
    except:
        #print('fmt="%m/%d/%Y"')
        fmt = dates.DateFormatter('%d/%m/%Y')

    plt.rcParams['xtick.labelsize'] = 8


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
        ax_3.plot(data[wave], label=wave)
    ax_3.xaxis.set_major_formatter(fmt)
    #ax_3.set_xlim(xx.min(), xx.max())
    ax_3.legend() # ncol = 7, fontsize = 9)
    ax_3.grid()
    ## save to file "ae33_bc_waves_week.png"
    if nfigs != 1:
        fig.savefig(path_to_figures + 'ae33_bc_waves_week.png', facecolor=facecolor) 


    ##########################
    ## Figure 4: BCff BCbb
    if nfigs == 1:
        ax_4 = fig.add_subplot(3, 2, 6)
    else:
        fig = plt.figure(figsize=(8, 4))
        ax_4 = fig.add_subplot(1, 1, 1)

    yy = data["BCff"]
    zz = data["BCbb"]
    ax_4.plot(yy, 'k', label='BCff')
    ax_4.plot(zz, 'orange', label='BCbb')
    ax_4.xaxis.set_major_formatter(fmt)
    #ax_4.set_xlim(xx.min(), xx.max())
    ax_4.legend()
    ax_4.grid()
    ## save to file "ae33_bc_week.png"
    if nfigs != 1:
        fig.savefig(path_to_figures + 'ae33_bc_week.png', facecolor=facecolor) 

    ## save four plots to file
    #fig.savefig('ae33_four_plots.png', facecolor='lightgray') # bbox_inches = 'tight'
    if nfigs == 1:
        print(path_to_figures + 'ae33_bc_four_plots.png')
        fig.savefig(path_to_figures + 'ae33_bc_four_plots.png', 
                    facecolor=facecolor,
                    #datetime_format='%d/%m/%Y'
                    ) # bbox_inches = 'tight'



if __name__ == "__main__":
    path_to_figures = "./figures/"
    
    # create one figure with four graphs
    plot_four_figures_from_excel('./data/table/2022_AE33-S08-01006.xlsx', path_to_figures )
    
    # create four figures
    plot_four_figures_from_excel('./data/table/2022_AE33-S08-01006.xlsx', path_to_figures, 4)
