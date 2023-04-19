import pyspark.pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')


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
#docstring for fill_null_str
fill_null_str.__doc__='''
fill_null_str used to replace the null values at ease for a string(object) column in the dataframe.
Can be thought as the single line code for filling the null values with the user's choice of string which makes more interactive.

Parameters
----------
data_frame : Data frame object, Pandas data frame object which is in 2-dimensional.
col_name : String, Column name of the data_frame to get the null values filled.
replace_str_with : String, Default 'Missing'
                   Value which should get replaced in place of null value.

Returns
-------
Series
  which contains all the values in the column after replacing the null values.

See Also
--------
transform.fill_null_num : Fills the null values in the numerical column in the dataframe.
transform.fill_null_data : Fill the null values in all the columns in the dataframe.

Examples
--------

>>> df=pd.DataFrame({'col1':[100,120],'col2':['Big data',None]})
>>> df
   col1      col2
0   100  Big data
1   120      None
>>> transform.fill_null_str(data_frame=df,col_name='col2')
0    Big data
1     Missing
Name: col2, dtype: object


Using the optional parameter replace_str_with

>>> df=pd.DataFrame({'col1':[100,120],'col2':['Big data',None]})
>>> df
   col1      col2
0   100  Big data
1   120      None
>>> transform.fill_null_str(data_frame=df,col_name='col2',replace_str_with='Null value')
0      Big data
1    Null value
Name: col2, dtype: object


Notifies if the string value in the replace_str_with parameter already exists in the column data.

>>> df=pd.DataFrame({'col1':[100,120],'col2':['Big data',None]})
>>> df
   col1      col2
0   100  Big data
1   120      None
>>> transform.fill_null_str(data_frame=df,col_name='col2',replace_str_with='Big data')
The value(Big data) already exists in the column: col2
Do you still want to replace with the given value [Y/n]?: y
0    Big data
1    Big data
Name: col2, dtype: object

'''



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

#documentation for fill_null_num
fill_null_num.__doc__='''

fill_null_num is used to replace the null values in the numerical column of the dataframe.
Filling the null values using a single line of code with the choice of imputation type.

Parameters
----------
data_frame : Data frame object, Pandas data frame object which is in 2-dimensional.
col_name : String, Numerical column name of the data_frame to get the null values filled.

impute_type : String, Default 'mode'
              Type of imputation i.e whether the null values in the column should be replaced
              with the mode or mean or median value of the column.

Returns
-------
Series
   which contains all the values of the column after replacing the null values.


See Also
--------
transform.fill_null_str : Fills the null values in the string(object) column in the dataframe.
transform.fill_null_data : Fill the null values in all the columns in the dataframe.

Examples
--------

>>> df=pd.DataFrame({'col1':[100,13,14,13,None],'col2':['Big data','DPA','ML',None,None]})
>>> df
    col1      col2
0  100.0  Big data
1   13.0       DPA
2   14.0        ML
3   13.0      None
4    NaN      None
>>> transform.fill_null_num(data_frame=df,col_name='col1')
0    100.0
1     13.0
2     14.0
3     13.0
4     13.0
Name: col1, dtype: float64


Changing the default value of impute_type to mean

>>> df=pd.DataFrame({'col1':[100,13,14,13,None],'col2':['Big data','DPA','ML',None,None]})
>>> df
    col1      col2
0  100.0  Big data
1   13.0       DPA
2   14.0        ML
3   13.0      None
4    NaN      None
>>> transform.fill_null_num(data_frame=df,col_name='col1',impute_type='mean')
0    100.0
1     13.0
2     14.0
3     13.0
4     35.0
Name: col1, dtype: float64

'''

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
                print("Couldn't convert the values for the column {} with datatype {}. Please consider to convert the values to string format if you want to treat it as object dtype before running this function".format(col_name,data[col_name].dtype))
        else:
            pass
    return data

#docstring for fill_null_data method
fill_null_data.__doc__='''
fill_null_data is used to replace the null values for both the object and
numerical columns in the dataframe.
This is based on the fill_null_str and fill_null_num methods.

Parameters
----------
data_frame : Data frame object, pandas data frame object which is in 2-Dimensional.
fill_null_sc : String value, default 'Missing'
               Takes the value(any) to replace with the null values in
               string columns of the data_frame.
nc_impute_type : String value, default 'mode'
                 Takes the value(mode,mean,median) to apply the kind of imputation
                 to replace the null values in numerical columns of the data_frame.

Returns
-------
A pandas data frame object, with no null values in the object and numerical columns.

See also
--------
transform.fill_null_str : Fills the null values in a string(object) column in the dataframe.
transform.fill_null_num : Fills the null values in a numerical column in the dataframe.


Examples
--------

>>> df=pd.DataFrame({'col1':[100,13,14,13,None],'col2':['Big data','DPA','ML',None,None]})
>>> df
    col1      col2
0  100.0  Big data
1   13.0       DPA
2   14.0        ML
3   13.0      None
4    NaN      None
>>> transform.fill_null_data(data_frame=df)
    col1      col2
0  100.0  Big data
1   13.0       DPA
2   14.0        ML
3   13.0   Missing
4   13.0   Missing

Changing the values for fill_null_sc and nc_impute_type

>>> df=pd.DataFrame({'col1':[100,13,14,13,None],'col2':['Big data','DPA','ML',None,None]})
>>> df
    col1      col2
0  100.0  Big data
1   13.0       DPA
2   14.0        ML
3   13.0      None
4    NaN      None
>>> transform.fill_null_data(data_frame=df,nc_impute_type='mean',fill_null_sc='Missing value')
    col1           col2
0  100.0       Big data
1   13.0            DPA
2   14.0             ML
3   13.0  Missing value
4   35.0  Missing value
>>> transform.fill_null_data(data_frame=df,nc_impute_type='median',fill_null_sc='Missing value')
    col1           col2
0  100.0       Big data
1   13.0            DPA
2   14.0             ML
3   13.0  Missing value
4   13.0  Missing value

'''

def strc_to_numc(data,col_name):
    col_data=data[col_name].copy()
    label_encode={None}
    if col_data.isnull().sum()>0:
        while True:
            print('-----------------------------------------------------------------------')
            print(col_name,' has null values. ','Please replace the null values.')
            replace_or_not=input('Do you want to the system to replace the null values for {} and then encode the values[Y/n]?: '.format(col_name))
            if replace_or_not.lower()=='y':
                print('------------------------------------------------')
                new_str=input('Enter the new value or press ENTER to assign the default value(missing): ')
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

#docstring for strc_to_numc
strc_to_numc.__doc__='''
strc_to_numc method is used to encode(label) the string values in
a column to the numerical values i.e Converts the entire object column's dtype to the numerical column.
This is also partially reliable on fill_null_str to replace the
null values in the column since it cannot encode the columns with the null values.

Parameters
----------

data : DataFrame object which is the 2-dimensional pandas data frame object.
col_name : String value
           Specific column's name you wish to encode the values in.

Returns
-------

A Series object
            Contains the encoded(numerical) values.
A dictionary object
                Contains the key, value pairs that references the numerical value(value) for
                each the string value(key) in the column.

Also see
--------
transform.strd_to_numd : Encodes all the columns in the entire with object dtype.

Examples
--------

>>> df=pd.DataFrame({'col1':['a','b','c','hello','hey'],'col2':[1,2,3,4,5]})
>>> df
    col1  col2
0      a     1
1      b     2
2      c     3
3  hello     4
4    hey     5
>>> transform.strc_to_numc(data=df,col_name='col1')
col1  executed
(0    0
1    1
2    2
3    3
4    4
Name: col1, dtype: object, {'a': 0, 'b': 1, 'c': 2, 'hello': 3, 'hey': 4})

'''



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

#docstring for strd_to_numd

strd_to_numd.__doc__='''
strd_to_numd is used to encode all the string values in the columns to numerical values by
applying labels for string values in columns.
This relies on strc_to_numc method to encode a single column values in the data frame.

Parameters
----------

data_frame : DataFrame object, which is the 2-dimensional pandas data frame object.

Returns
-------
A data frame object
             contains all the intial columns where the string(object) columns are encoded to
             numerical values.
A dictionary object
             contains the dictionaries of each column's values and thier labels returned from
             strc_to_numc.

Also see
--------
strc_to_numc : Encodes the string values in the column to numerical values.

'''
