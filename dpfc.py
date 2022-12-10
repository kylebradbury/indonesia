''' dpfc.py

Module for importing Data Portal For Cities data (https://dataportalforcities.org/)
The excel spreadsheets with one year of data should be imported into a single
directory prior to import. The functions below will read, select key columns, 
and reformat the data for use.

Author: Kyle Bradbury
'''

from numpy import NaN
import pandas as pd
import os

def extract_dpfc_data(file_path):
    ''' Read in the DPFC data from a single excel file
    '''
    df = pd.read_excel(file_path,
                    sheet_name="eCRF_3",
                    usecols='D,F,H',
                    index_col=False
    )
    df = df.fillna(0)
    return df

def extract_dpfc_metadata(file_path):
    ''' Read in the DPFC data from a single excel file and extract the municipality name
    '''
    df = pd.read_excel(file_path,
                       sheet_name="eCRF_1",
                       index_col=False,
                       header=None
    )
    return {'city': df[2][1], 'city_id': df[2][4], 'year': df[2][6]}

def convert_dpfc_data(df):
    ''' Reformat the data to have one row per city (rather than 4 in the native
    format) and add the RES-, COM-, etc., use modifier to each fuel type. This
    Should be run AFTER extract_dpfc_data()
    '''
    fuels = {
        'electricity' : ['Electricity'],
        'district_heating' : ['District heating - hot water', 'District heating - steam', 'District Cooling'],
        'coal' : ['Coal (Bituminous or Black coal)'],
        'oil' : ['Diesel oil', 'Kerosene'],
        'natural_gas' : ['Natural gas'],
        'lpg' : ['Liquefied Petroleum Gas (LPG)'],
        'kerosene' : ['Kerosene'],
        'biomass' : ['Wood or wood waste','Other biogas','Other Liquid BioFuels']
    }

    sectors = {
        'Residential Buildings': 'RES',
        'Commercial Buildings': 'COM',
        'Institutional Buildings': 'MUN',
        'Industry': 'IND',
        'Agriculture, Forestry and Fisheries': 'AFF',
    }

    entry = {}
    for sect in sectors:
        data = df[df['CRF - Sub-sector'] == sect]
        for category in fuels:
            total_energy = 0
            for ftype in fuels[category]:
                if ftype in data['Fuel type or activity'].values:
                    total_energy += data[data['Fuel type or activity'] == ftype]['Activity data - Amount'].values[0]
            entry[f'{sectors[sect]}-{category}'] = total_energy
                
    return entry

def merge_dpfc_metadata(data_dict,metadata_dict):
    ''' Combines the actual data and meta data by combining the outputs from 
    extract_dpfc_metadata() and convert_dpfc_data()
    '''
    
    # Merge the two dictionaries
    data_dict.update(metadata_dict)
    
    df = pd.DataFrame(data_dict, index=[0])
    col_order = ['city', 'year', 'city_id', 'RES-electricity', 'COM-electricity',
       'MUN-electricity', 'IND-electricity', 'AFF-electricity', 'RES-coal',
       'COM-coal', 'MUN-coal', 'IND-coal', 'AFF-coal', 'RES-natural_gas',
       'COM-natural_gas', 'MUN-natural_gas', 'IND-natural_gas',
       'AFF-natural_gas', 'RES-oil', 'COM-oil', 'MUN-oil', 'IND-oil',
       'AFF-oil', 'RES-kerosene', 'COM-kerosene', 'MUN-kerosene',
       'IND-kerosene', 'AFF-kerosene', 'RES-lpg', 'COM-lpg', 'MUN-lpg',
       'IND-lpg', 'AFF-lpg', 'RES-biomass', 'COM-biomass', 'MUN-biomass',
       'IND-biomass', 'AFF-biomass', 'RES-district_heating',
       'COM-district_heating', 'MUN-district_heating', 'IND-district_heating',
       'AFF-district_heating']
    df = df[col_order]
    return df

# MAIN FILE
def produce_ground_truth(location,dir_path):
    ''' Processes every file in the stated directory into a single file with one
    line per city. Outputs the combined data to a single CSV file at `dir_path`
    labeled with the location, `location`. THIS IS THE MAIN FILE THAT SHOULD BE 
    RUN FOR EXTRACTING THE DATA
    '''
    directory = f'{dir_path}ground_truth_{location}/'
    files = os.listdir(directory)
    df = pd.DataFrame()
    for file in files:
        file_path = os.path.join(directory,file)
        data = extract_dpfc_data(file_path)
        data_dict = convert_dpfc_data(data)
        metadata_dict = extract_dpfc_metadata(file_path)
        muni_data = merge_dpfc_metadata(data_dict,metadata_dict)
        df = pd.concat([df,muni_data])
    df.to_csv(f'./processed_data/ground_truth_{location}.csv', index=False)
    return df