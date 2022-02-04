#!/usr/bin/python3
''' 
Operating system agnotstic script by Daniel Dominguez for FSU EOAS
Combines weather station and hydrology well data from the data portal.

Type "<script_name> -h" for help 

General alogrithm

1. Parse arguments
2. Generate time period
3. Download data sets within time period into temp folder
4. concatenate and bind datasets
4.5. Adjust the well water height, pressure, and atm differential
     -The sonde is 87 feet from the top of the well casing (3ft above the ground)
     -The HL7_raw_depth measure is the weight of water and a small amount of atmosphere above the sonde
     -Therefore, you need to factor into the depth, the differential water height due to ATMOS_pa
      and then you need to add the water column depth to the measure to get True_well_depth
     
     i.e. Sonde measure 2.5meters, the pressure height diff for this moment is +30cm, and the Column_depth constant is Convert_to_meters(148ft - 87ft) = 18.5928 Meters (61ft),
          True_well_depth = 2.5m + .3m + 18.59m = 21.7m 

     -the differential is generated. Pa_differential -> (HL7_FIXED_PA - WxPRO_ATMOS_PA); then Pressure_height_diff = Convert_pa_to_water_depth_in_meters(Pa_differential);
      Finally True_well_depth = HL7_raw_depth + Column_depth + Pressure_height_diff

      NOTE: 1 meter sea water ~ .1bar (diver convention)

      NOTE2: Pressure = Liquid_height * Liquid_density_l * gravity; where Liquid_density_l = density_l * specific_specific_gravity_l
             Works for any liquid
             Freshwater denisty at 25c = .997 g/cm^3 (USGS)

5. save final dataset as output
6. Clean up temp folder

'''

import os
import wget
from pathlib import Path
import datetime as dt
import tempfile
import shutil
import pandas as pd
import numpy as np
import re
import sys
import argparse
#library for unit conversion
import pint

HL7_column_depth = 18.59 #meters below the HL& sonde sensors -a constant


### Add argument parser ###

parser = argparse.ArgumentParser(description='Scrape/Concatenate WxPro & Hydrology Well Data')
parser.add_argument('-f', '--filename', metavar="<filename>", type=str, required=True,
                    help='Filename of output file')
parser.add_argument('-n', '--ndays', type=int, default=None,
                    help='number of days of records to concatenate')
parser.add_argument('-s', '--start-date', metavar="<Strt-date>", type=str, required=False,
                    help='Date String in ISO format "YYYY-MM-DD"')
parser.add_argument('-e', '--end-date', metavar="<Stp-date>", type=str, required=False,
                    help='Date String in ISO format "YYYY-MM-DD"')

args = parser.parse_args()
filename = args.filename


if (args.start_date is not None and args.ndays is None and args.end_date is None):
    end_date = dt.date.today() 
    try:
        start_date  = dt.date.fromisoformat(args.start_date)    
    except:
        print("CHECK DATE FORMAT YYYY-MM-DD")
elif (args.start_date is not None and args.end_date is not None and args.ndays is None):
    try:
        start_date  = dt.date.fromisoformat(args.start_date)
        end_date  = dt.date.fromisoformat(args.end_date)
    except:
        print("CHECK DATE FORMAT YYYY-MM-DD") 
elif (args.start_date is not None and args.end_date is None and args.ndays is not None):
    try:
        start_date  = dt.date.fromisoformat(args.start_date)    
    except:
        print("CHECK DATE FORMAT YYYY-MM-DD")
    end_date = start_date + dt.timedelta(args.ndays)
elif (args.start_date is None and args.end_date is not None and args.ndays is not None):
    try:
        end_date  = dt.date.fromisoformat(args.end_date)    
    except:
        print("CHECK DATE FORMAT YYYY-MM-DD")
    start_date = end_date - dt.timedelta(args.ndays)
elif (args.start_date is None and args.end_date is not None and args.ndays is None):
    print("Ambigious Case, provide start date or n_days")
    raise SystemExit(0)
elif (args.start_date is None and args.end_date is None and args.ndays is not None):
    end_date = end_date = dt.date.today() 
    start_date = end_date - dt.timedelta(args.ndays-1)
 

def daterange(start_date, end_date):
    ''' yeilds date iteration without list '''
    for n in range(int((end_date - start_date).days + 1)):
        yield start_date + dt.timedelta(n)

def zero_num(x):
    ''' Zero pads integers less than 10 '''
    if (int(x) <= 9):
        return f'0{x}'
    else:
        return x


wxpro_url = "http://inst.eoas.fsu.edu/RAID/WeatherStation/"

#lambda function to create filename from dt elements
wxpro_fl = lambda m, d, y : f'WXPRO_{y}_{m}_{d}.csv'

hydro_url = "http://inst.eoas.fsu.edu/RAID/hydrowell/"

hydro_fl = lambda m, d, y : f'{y}-{m}-{d}_RESAMP.csv'


#Set up temporary directories
hydro_folder_t = tempfile.mkdtemp()
wxpro_folder_t = tempfile.mkdtemp()
start_folder = Path(os.getcwd())

#Render them OS agnostic
hydro_folder = Path(hydro_folder_t)
wxpro_folder = Path(wxpro_folder_t)


print("Begin downloading datasets to temp folder")

for single_date in daterange(start_date, end_date):
    hydro_name = hydro_fl(zero_num(single_date.month), zero_num(single_date.day), zero_num(single_date.year))
    wxpro_name = wxpro_fl(zero_num(single_date.month), zero_num(single_date.day), zero_num(single_date.year))
    
    try:
        wget.download(f"{hydro_url}{hydro_name}", out=str(hydro_folder))
    except:
        pass
    try:
        wget.download(f"{wxpro_url}{wxpro_name}", out=str(wxpro_folder))
    except:
        pass

print("\nDone... \nBegin concatenating datasets")

HL7_columns = ["Datetime", 'TEMP_C', 'PRESSURE_mmhg', 'VOLTAGE', 'DEPTH_m', 'DISOLVEDO2_%sat', 'PH', 'SPECIFICCONDUCTIVITY_milisiemensPercm', 'SPECIFICGRAVITY', 'TOTALDISSOLVEDSOLIDS_gramPerLiter', 'na']
os.chdir(hydro_folder)

csvs =  os.listdir()
csvs.sort(reverse=True)
concat_list = [pd.read_csv(file) for file in csvs]
well_data = pd.concat(concat_list)
#adjust the columns
well_data["datetime"] = pd.to_datetime(well_data["datetime"], format = "%Y-%m-%d %H:%M:%S")
well_data = well_data.sort_values(by="datetime")
well_data.set_index('datetime', inplace=True)
well_data1 = well_data.resample("15min").mean()
well_data1 = well_data1.reset_index()
well_data1 = pd.DataFrame(well_data1)
well_data1.columns = HL7_columns


wxpro_columns = ['Datetime','RecordNumber','Batt_V','PTemp_C','Flux_Wattspersqrmeter','Raw_flux_milivolts','cs_temp','x_or','y_or','z_or','PRESSURE_mbar','Temp_C','RelHumidity','WindSpeed_mPers','WindDir_0North','Rain_mmTotal']
os.chdir(wxpro_folder)

csvs =  os.listdir()
csvs.sort(reverse=True)
concat_list = [pd.read_csv(file) for file in csvs]
wx_data = pd.concat(concat_list)
wx_data["Datetime"] = pd.to_datetime(wx_data["Datetime"], format = "%Y-%m-%d %H:%M:%S")
wx_data.set_index('Datetime', inplace=True)
wx_data3 = wx_data.resample("15min").mean()
wx_data3 = wx_data3.reset_index()
wx_data3 = pd.DataFrame(wx_data3)
wx_data3.columns = wxpro_columns


### Filter out bad columns ####
print("Done... \nBegin join datasets")

final = wx_data3.join(well_data1.set_index("Datetime"), on="Datetime")

print("Fixing pressure readings and Save")
reg = pint.UnitRegistry()

final["uncorrected_depth"] = final['DEPTH_m'] + HL7_column_depth

final["Gauge_pressure_diff_mbar"] = 0

#For loop based on number of rows in table. .iloc function takes [row_num, column_num]
for x in range(final.shape[0]):
    final.iloc[x, 27] = (((final.iloc[x, 17] * reg.mmHg)  - ((final.iloc[x, 10]/1000) * reg.bar))).to('bar').magnitude * 1000

#print(final.iloc[0, 10])  #AIR pressure_mbar
#print(final.iloc[0, 17])  #HL7 Pressure_mmhg Constant
#print(final.iloc[0, 27])  #HL7 Guage_pressure_diff

#9.970474 * 9.806   (density water * 10) * gravity or:
#1mbar = .0980665 mm H20 Fresh
#.0980665mm * 1000mm/m = 98.0665 m
  
final["Gauge_depth_meters"] = (((final["Gauge_pressure_diff_mbar"]) / (98.0665)) + final["DEPTH_m"]) + HL7_column_depth  #pa / density of fresh water * gravity acceleration

os.chdir(start_folder)

final.to_csv(filename)

shutil.rmtree(hydro_folder)
shutil.rmtree(wxpro_folder)
print("Done, garbage collected")
