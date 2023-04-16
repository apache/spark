import pyspark.pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

def strc_to_numc(data,col_name):
    col_data=data[col_name].copy()
    label_encode={None}
    if col_data.isnull().sum()>0:
        while True:
            print('-----------------------------------------------------------------------')
            print(col_name,' has null values. ','Please replace the null values.')
            #replace_or_not='y'
            replace_or_not=input('Do you want to the system to replace the null values for {} and then encode the values[Y/n]?: '.format(col_name))
            if replace_or_not.lower()=='y':
                print('------------------------------------------------')
                new_str=input('Enter the new value or press ENTER to assign the default value(missing): ')
                #new_str='Missing'
                if len(new_str)==0:
                    new_str='Missing'
                print('Replace the values')
                col_data=fill_null_str(data,col_name,replace_str_with=new_str)
                #encoding
                keys=np.unique(col_data.sort_values().to_numpy())
                values=range(len(keys))
                label_encode=dict(zip(keys,values))
                encoded_values=col_data.map(label_encode)
                return encoded_values,label_encode
                break
            elif replace_or_not.lower()=='n':
                print('Do not replace the values')
                return col_data,label_encode
                break
            else:
                print('Invalid Option')
                pass
    else:
        #encoding
        print(col_name,' executed')
        keys=np.unique(col_data.sort_values().to_numpy())
        values=range(len(keys))
        label_encode=dict(zip(keys,values))
        encoded_values=col_data.map(label_encode)
        return encoded_values,label_encode


#converting all the string columns in the dataframe to numeric columns
def strd_to_numd(data_frame):
    dictionary_values={}
    data=data_frame.copy()
    for col_name in data.columns:
        if data[col_name].dtype=='O':
            try:
                converted_values=strc_to_numc(data,col_name)
                data[col_name]=converted_values[0]
                dictionary_values[col_name]=converted_values[1]#encoded values
            except:
                data[col_name]=data[col_name].astype(str)
                converted_values=strc_to_numc(data,col_name)
                data[col_name]=converted_values[0]
                dictionary_values[col_name]=converted_values[1]#encoded values
        else:
            pass
    return data,dictionary_values

#replacing the null values
def fill_null_str(data_frame,col_name,replace_str_with='Missing'):
    col_data=data_frame[col_name].copy()
    while True:
        if replace_str_with in np.unique(col_data.dropna().unique().to_numpy()):
            print('The value({}) already exists in the column: {}'.format(replace_str_with,col_name))
            ow_or_not=input('Do you still want to replace with the given value [Y/n]?: ')
            if ow_or_not.lower()=='y':
                replaced_col=col_data.fillna(replace_str_with)
                return replaced_col
                break
            elif ow_or_not.lower()=='n':
                new_value=input('Enter the non-existing value to replace: ')
                if new_value in np.unique(col_data.unique().to_numpy()):
                    pass
                else:
                    replaced_col=col_data.fillna(replace_str_with)
                    return replaced_col
                    break
            else:
                print('Invalid input')
        else:
            replaced_col=col_data.fillna(replace_str_with)
            return replaced_col


def fill_null_num(data_frame,col_name,impute_type='mode'):
    if impute_type=='mode':
        if len(data_frame[col_name].value_counts())==0:
            replaced_col=data_frame[col_name].fillna(0)
            return replaced_col
        else:
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
                print("Couldn't convert the values for the columns {} with datatype {}".format(col_name,data[col_name].dtype))
        else:
            pass
    return data
