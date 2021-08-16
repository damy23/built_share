import json
import sys,os,re,io
from numpy import NaN, dtype, record, unique
import pandas as pd
import csv
from pandas import ExcelWriter
from pandas import ExcelFile
from pandas.core import series
import requests
from pandas.io.json import json_normalize
from contextlib import suppress
import numpy as np
import datetime 
from functools import reduce


'''
exectuion - just place script in local and run. 

python3 Restaurant_DataAnalysis.py

'''


def dict_generator(indict, pre=None):
    pre = pre[:] if pre else []
    if isinstance(indict, dict):
        for key, value in indict.items():
            if isinstance(value, dict):
                for d in dict_generator(value,  pre + [key]):
                    return d
            elif isinstance(value, list) or isinstance(value, tuple):
                for k,v in enumerate(value):
                    for d in dict_generator(v, pre + [key] + [k]):
                        return d
            else:
                return pre + [key, value]
                #print(pre)
    else:
        return indict

def file_rmv():
    #enable when it deploy into lambda - Currently files will generate in temp location 
    if os.path.exists('/tmp/'+"01_Restaurants_Data_extraction.csv"):
        os.remove('/tmp/'+"01_Restaurants_Data_extraction.csv")
    if os.path.exists('/tmp/'+"02_Event_ResData_extraction.csv"):
        os.remove('/tmp/'+"02_Event_ResData_extraction.csv")
    
    #if os.path.exists("01_Restaurants_Data_extraction.csv"):
        #os.remove("01_Restaurants_Data_extraction.csv")
    #if os.path.exists("02_Event_ResData_extraction.csv"):
        #os.remove("02_Event_ResData_extraction.csv")

def __download_json():
    download_res_json = requests.get('https://raw.githubusercontent.com/ashraf356/cc4braininterview/main/restaurant_data.json')
    set_binary_file = download_res_json.content
    data = json.loads(set_binary_file)
    return data

def __country_lkp_excel():
    read_excel_url = 'https://github.com/ashraf356/cc4braininterview/blob/main/Country-Code.xlsx?raw=true'
    cntry_content = requests.get(read_excel_url).content
    df_excel = pd.read_excel(cntry_content, sheet_name ='Sheet1', engine='openpyxl')
    #df_excel = pd.read_excel('Country-Code.xlsx', sheet_name ='Sheet1', engine='openpyxl')
    df_cntry = pd.DataFrame(df_excel)
    return df_cntry


def resturant_extract_data(data):
    df = pd.DataFrame(data)
    file_rmv()
    try:
        row = 0
        for row in range(len(df["restaurants"])):
            res_df=df["restaurants"][row]
            df_jnormalize = pd.json_normalize(res_df)
            df_jnormalize.columns = df_jnormalize.columns.map(lambda x: x.split(".")[-1])
            if df_jnormalize.isnull().any().any() != 'True':
                df_cntry = __country_lkp_excel()
                df_cntry.columns = df_cntry.columns.map(lambda x: x.split(".")[-1])
                try:
                    df_merged = pd.merge(df_jnormalize,df_cntry,left_on=df_jnormalize['country_id'], right_on=df_cntry['Country Code'], how = 'left')
                except:
                    None
                header_cols=["Restaurant_id","Restaurant_Name","Country_name","City name","User_rating_votes","User_aggregate_rating","Cuisines"]
                ## enable for lambda tmp storage 
                #df_merged[["res_id","name","Country","city","votes","aggregate_rating","cuisines"]].to_csv('/tmp/'+"01_Restaurants_Data_extraction.csv",mode = 'a',header=header_cols,index=False)
                df_merged[["res_id","name","Country","city","votes","aggregate_rating","cuisines"]].to_csv("01_Restaurants_Data_extraction.csv",mode = 'a',header=header_cols,index=False)             
    except:
        None


def event_extraction_call(data):
    df=pd.DataFrame(data)
    for row in range(len(df["restaurants"])): 
        res_df=df["restaurants"][row]
        try:
            call = dict_generator(res_df)
            cal = pd.json_normalize(call)
            for row1 in range(len(cal['restaurant.zomato_events'])):
                res_df2=cal['restaurant.zomato_events'][row1]
                call2 = dict_generator(res_df2)
                cal2 = pd.json_normalize(call2)
                for i in range(len(cal['restaurant.R.res_id'])):
                    search=r"\w\S*{}".format(cal['restaurant.R.res_id'][i])
                    search_result = cal2["event.share_url"].str.contains(search).values
                    skey_unique = np.unique(search_result)
                    for _search_key in skey_unique:
                        if skey_unique[_search_key].size > 0:
                            #cal2['ref_key_res_id']=cal2['event.event_id'].apply(lambda x: cal['restaurant.R.res_id'][i] if x )
                            assign= cal2.assign(**{'event_ref_key_res_id': cal['restaurant.R.res_id'][i]})
                            ''' Date filter part yet to compelted 
                            #assign['event.start_date'] = pd.to_datetime(assign['event.start_date'], format='%Y-%m-%d')
                            #assign['event.end_date'] = pd.to_datetime(assign['event.end_date'], format='%Y-%m-%d')
                            #assign = assign.query("event.start_date >= '2017-03-31' and event.end_date < '2017-05-01'") ## date filter will need to work
                            #else:
                            #assign= cal2.assign({'event_ref_key_res_id': NaN})
                            '''
                            for row2 in range(len(cal2['event.photos'])):
                                res_df3=cal2['event.photos'][row2]
                                call3 = dict_generator(res_df3)
                                cal3 = pd.json_normalize(call3)
        except(TypeError,KeyError):
            row=row+1

        dfs = [cal,assign,cal3]
        nan_value = 'NA'
        result_2 = reduce(lambda df_left,df_right: pd.merge(df_left, df_right, left_index=False, right_index=False, how='cross'), dfs).fillna(nan_value)
        result_2.columns = result_2.columns.map(lambda x: x.split(".")[-1])
        if len(result_2) != 0:
            header_col=["Event_id","Restaurant_id","Restaurant_name","Photo_url","Event_title","Event_start_date","Event_end_date"]
            result_2[["event_id","res_id","name","photos_url","title","start_date","end_date"]].to_csv("02_Event_ResData_extraction.csv",mode = 'a',header=header_col,index=False)
            ## enable for lambda tmp storage 
            #result_2[["event_id","res_id","name","photos_url","title","start_date","end_date"]].to_csv('/tmp/'+"02_Event_ResData_extraction.csv",mode = 'a',header=header_col,index=False)
            
    return


def lambda_handler(event,context):
    data = __download_json()
    resturant_extract_data(data)
    event_extraction_call(data)
    return


if __name__ == "__main__":
    lambda_handler('json_rest','rest_api')