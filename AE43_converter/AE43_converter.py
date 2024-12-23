## need to make "pip install openpyxl==3.0.9"
## pip install pandas

#### import modules
import pandas as pd
from datetime import datetime, date, time
import os
#import sys


xlscolumns = ['Datetime', 'BC1', 'BC2', 'BC3', 'BC4', 'BC5', 'BC6', 'BC7', 'BB(%)']

## +
## https://pythonist.ru/preobrazovanie-strok-v-datu-so-vremenem/
def select_year_month(datastring):
    if '.' in datastring:
        sep = '.'
        return "_".join([x for x in datastring.split()[0].split(sep)[2:0:-1]])
    elif '-' in datastring:
        sep = '-'
        return "_".join([x for x in datastring.split()[0].split(sep)[:2]])
    else:
        sep = '/'
    return "_".join([x for x in datastring.split()[0].split(sep)[2:0:-1]])
    #return "_".join([x for x in datastring.split()[0].split('/')[:2]])
    #return datastring.split('/')[0] + '_' + datastring.split('/')[1]


## +
## ---- open AE43 file ----
def read_AE43_csv_file(filename):
    #head = 'Date(yyyy/MM/dd); Time(hh:mm:ss); Timebase; RefCh1; Sen1Ch1; Sen2Ch1; RefCh2; Sen1Ch2; Sen2Ch2; RefCh3; Sen1Ch3; Sen2Ch3; RefCh4; Sen1Ch4; Sen2Ch4; RefCh5; Sen1Ch5; Sen2Ch5; RefCh6; Sen1Ch6; Sen2Ch6; RefCh7; Sen1Ch7; Sen2Ch7; Flow1; Flow2; FlowC; Pressure(Pa); Temperature(�C); BB(%); ContTemp; SupplyTemp; Status; ContStatus; DetectStatus; LedStatus; ValveStatus; LedTemp; BC11; BC12; BC1; BC21; BC22; BC2; BC31; BC32; BC3; BC41; BC42; BC4; BC51; BC52; BC5; BC61; BC62; BC6; BC71; BC72; BC7; K1; K2; K3; K4; K5; K6; K7; TapeAdvCount; '
    #columns = head.split("; ")[:-1] + [str(i) for i in range(1, 10)]
    #print(len(columns), end=' ')

    ## read data
    datum = pd.read_csv(filename, index_col=0, #names=columns, 
                        #encoding='windows-1252'
                        )

    ## columns to dataframe 
    #params = ['Date(yyyy/MM/dd)', 'Time(hh:mm:ss)', 'BC1', 'BC2', 'BC3', 'BC4', 'BC5', 'BC6', 'BC7', 'BB(%)']
    #datum = datum[params]
    #datum['Datetime'] = datum['Date(yyyy/MM/dd)'] + ' ' + datum['Time(hh:mm:ss)']
    datum['Datetime'] = datum['EndTime'] #.apply(lambda x: ".".join(x.split('/')[::-1]))
                      #+ ' ' \
                      #+ datum['Time(hh:mm:ss)'].apply(lambda x: ':'.join(x.split(':')[:2]))
    datum['BB(%)'] = datum['BB'].astype(float)
                      
    #print(datum.head())
    return datum[xlscolumns]


## +
## ---- open ddat file ----
def read_ddat_file(filename):
    head = 'Date(yyyy/MM/dd); Time(hh:mm:ss); Timebase; RefCh1; Sen1Ch1; Sen2Ch1; RefCh2; Sen1Ch2; Sen2Ch2; RefCh3; Sen1Ch3; Sen2Ch3; RefCh4; Sen1Ch4; Sen2Ch4; RefCh5; Sen1Ch5; Sen2Ch5; RefCh6; Sen1Ch6; Sen2Ch6; RefCh7; Sen1Ch7; Sen2Ch7; Flow1; Flow2; FlowC; Pressure(Pa); Temperature(�C); BB(%); ContTemp; SupplyTemp; Status; ContStatus; DetectStatus; LedStatus; ValveStatus; LedTemp; BC11; BC12; BC1; BC21; BC22; BC2; BC31; BC32; BC3; BC41; BC42; BC4; BC51; BC52; BC5; BC61; BC62; BC6; BC71; BC72; BC7; K1; K2; K3; K4; K5; K6; K7; TapeAdvCount; '
    columns = head.split("; ")[:-1] + [str(i) for i in range(1, 10)]
    #print(len(columns), end=' ')

    ## read data
    datum = pd.read_csv(filename, sep='\s+', on_bad_lines='warn', 
                        skiprows=7,  skip_blank_lines=True,
                        index_col=None, names=columns, 
                        encoding='windows-1252')

    ## columns to dataframe 
    #params = ['Date(yyyy/MM/dd)', 'Time(hh:mm:ss)', 'BC1', 'BC2', 'BC3', 'BC4', 'BC5', 'BC6', 'BC7', 'BB(%)']
    #datum = datum[params]
    #datum['Datetime'] = datum['Date(yyyy/MM/dd)'] + ' ' + datum['Time(hh:mm:ss)']
    datum['Datetime'] = datum['Date(yyyy/MM/dd)'].apply(lambda x: ".".join(x.split('/')[::-1])) \
                      + ' ' \
                      + datum['Time(hh:mm:ss)'].apply(lambda x: ':'.join(x.split(':')[:2]))
    return datum[xlscolumns]


## +
def read_dataframe_from_excel_file(xlsfilename):
        #columns = ['Date', 'Time (Moscow)', 'BC1', 'BC2', 'BC3', 'BC4', 'BC5', 'BC6',
        #    'BC7', 'BB(%)', 'BCbb', 'BCff', 'Date.1', 'Time (Moscow).1']
        columns = xlscolumns
        create_new = False

        try:
            ## read excel file to dataframe
            ## need to make "pip install openpyxl==3.0.9" if there are problems with excel file reading
            datum = pd.read_excel(xlsfilename)
            #print(xlsfilename, "read,   table: ", datum.shape)
            #print(datum.head(2))
            if datum.shape[1] != len(columns) + 2:
                print(f"WARNING!!! File {xlsfilename} has old format, is ignoring!!!")
                point = xlsfilename.rfind('.')
                os.rename(xlsfilename, xlsfilename[:point] + "_old" +  xlsfilename[point:])
                create_new = True
        except:
            create_new = True

        if create_new:
            # create new dummy dataframe
            datum = pd.DataFrame(columns=columns)
            print("No file", xlsfilename, "  New file will created")

        return datum


### write dataframe to excel file
### \return - no return
### +
def write_dataframe_to_excel_file(dataframe, ae_name):
    """ write dataframe to excel file """
    ## add columns
    dataframe['BCbb'] = dataframe['BB(%)'].astype(float) \
                      * dataframe['BC5'].astype(float) / 100
    dataframe['BCff'] = (100 - dataframe['BB(%)'].astype(float)) / 100 \
                      * dataframe['BC5'].astype(float)
    #dataframe['Date.1'] = dataframe['Date']
    #dataframe['Time (Moscow).1'] = dataframe['Time (Moscow)']

    #### extract year and month from data
    year_month = dataframe['Datetime'].apply(select_year_month).unique()
    ## print(year_month)

    ### prepare directory
    table_dirname = dirname + 'xls' + sep
    if not os.path.isdir(table_dirname):
        os.makedirs(table_dirname)


    #### write to excel file
    for ym_pattern in year_month:
        ## print(ym_pattern, end=' ')
        filenamexls = table_dirname + ym_pattern + '_' + ae_name + ".xlsx"
        filenamecsv = table_dirname + ym_pattern + '_' + ae_name + ".csv"

        ## отфильтровать строки за нужный месяц и год
        dfsave = dataframe[dataframe['Datetime'].apply(select_year_month) == ym_pattern]
        ## print(ym_pattern, ": ", dfsave.shape)

        ##### try to open excel file #####
        ## read or cleate datafame
        xlsdata = read_dataframe_from_excel_file(filenamexls)
        if xlsdata.shape[0]:
            #xlsdata.append(dfsave, ignore_index=True).drop_duplicates()
            dfsave = pd.concat([xlsdata, dfsave], ignore_index=True)\
                    .drop_duplicates(subset=['Datetime'])\
                    .sort_values(by=['Datetime']) 

        dfsave.set_index('Datetime').to_excel(filenamexls, engine='openpyxl')
        dfsave.set_index('Datetime').to_csv(filenamecsv)


def read_every_day_files(dirname):
    # create пустой датафрейм
    params = xlscolumns
    data = pd.DataFrame(columns=params)

    # перебрать все файлы и считать из них
    year = 2022
    for month in ['02', '03']:
        for day in range(1, 32):
            filename = dirname + 'AE33_' + ae_name + '_' \
                    + f"{year}{month}{day:02d}.dat" 

            ## check file exists
            try:
                os.stat(filename)
                print(filename, end=' ')
            except:
                #print("No ", filename)
                continue

            ## check numbers in line if != 65 - print error
            lens = set(len(line.split()) for line in open(filename).readlines()[8:])
            if len(lens) > 1:
                print('has line with different numbers:', *lens)
                #continue

            df = read_ddat_file(filename)
            #print("df: ", df.shape)
            data = pd.concat([data, df], ignore_index=True)
    return data



if __name__ == '__main__':
    # Execute when the module is not initialized from an import statement.
    ### ======================================================
    ### Read AE43 csv data file
    ### 
    if 'ix' in os.name:
            sep = '/'  ## -- path separator for LINIX
    else:
        sep = '\\' ## -- path separator for Windows
        
    ae_name = "AE43-default" # "AE43-S01-00125"
    dirname = '.' + sep + 'data'
    
    print("Это программа для конвертации данных прибора Aethalometer-43 из формата прибора в читаемый файл.")
    
    
    if not os.path.isdir(dirname):
        print(f"\n\n Error Alarm!! Папка с данными не обнаружена: {dirname}!")
        print(f"Создайте папку {dirname}, положите туда файлы с данными. После этого снова запустите эту программу.\n")
        exit()
        
    if not dirname.endswith('/'):
        dirname = dirname + sep
   
    #print(*os.listdir(dirname), sep='\n')
    for filename in os.listdir(dirname):
        ## проверить файл ли это
        if not os.path.isfile(dirname + filename):
            continue
        if filename[-3:] != 'csv':
            continue
        if not filename.startswith("AE43"):
            continue
            
        ae_name = filename.split("_")[0]
 
        data = read_AE43_csv_file(dirname + filename) 
        print(f"\nОбрабатывается файл {dirname + filename} data: ", data.shape)
        write_dataframe_to_excel_file(data, ae_name)

    x = input("Press ENTER for finish the programm ...\n")
    