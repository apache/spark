import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
#converting the string data column to numeric data column
def strc_to_numc(data_frame,col_name):
    if data_frame[col_name].isnull().sum()>0:
        print(col_name,' has null values. ','Please replace the null values.')
        return data_frame[col_name]
        #create a option to take the input whether to replace the values or not. If yes, call fill_null_data or fill_null_str or fill_null_num functions
    else:
        #print(col_name,' executed')
        keys=np.unique(data_frame[col_name].sort_values().to_numpy())
        values=range(len(keys))
        label_encode=dict(zip(keys,values))
        encoded_values=data_frame[col_name].map(label_encode)
        return encoded_values

#converting all the string columns in the dataframe to numeric columns
def strd_to_numd(data_frame):
    data=data_frame.copy()
    for col_name in data.columns:
        if data[col_name].dtype=='O':
            data[col_name]=strc_to_numc(data,col_name)
    return data

#replacing the null values
def fill_null_str(data_frame,col_name,replace_str_with='Missing'):
    replaced_col=data_frame[col_name].fillna(replace_str_with)
    return replaced_col

def fill_null_num(data_frame,col_name,impute_type='mode'):
    #print(col_name)
    if impute_type=='mode':
        replaced_col=data_frame[col_name].fillna(data_frame[col_name].mode()[0].astype(float))
        return replaced_col
    elif impute_type=='median':
        replaced_col=data_frame[col_name].fillna(data_frame[col_name].median())
        return replaced_col
    elif impute_type=='mean':
        replaced_col=data_frame[col_name].fillna(data_frame[col_name].mean())
        return replaced_col
    else:
        print('Impute type is not valid')


def fill_null_data(data_frame,fill_null_sc='Missing',nc_impute_type='mode'):
    data=data_frame.copy()
    for col_name in data.columns:
        if data_frame[col_name].isnull().sum()>0:
            try:
                if data[col_name].dtype=='O':
                    data[col_name]=fill_null_str(data,col_name,fill_null_sc)
                else:
                    data[col_name]=fill_null_num(data,col_name,nc_impute_type)
            except:
                print("Couldn't replace the null values for ",col_name)
        else:
            pass
    return data
