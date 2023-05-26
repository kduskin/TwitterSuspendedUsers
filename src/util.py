import numpy as np
import arviz as az 
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import stan
from os.path import exists
import pickle
import os
import json
from multiprocessing import Pool
import pickle 
import glob
import itertools
import nest_asyncio
import time
import string

from src.model import *
from src.util import *
from src.plotting import *

def get_median_change(group):
    full_post = np.empty((4000,group[1].shape[0]))
    for idx in range(group[1].shape[0]):
        idata = az.from_json(group[1]['posterior_location'].values[idx])
        counter_factual = np.median(np.exp(idata.posterior['mu0'] + idata.posterior['b0'] * 1))
        actual = np.exp(idata.posterior['mu0'] + idata.posterior['b0'] * 1 + idata.posterior['mu1'] + idata.posterior['b2'] * 1)
        t = (actual - counter_factual) / counter_factual
        t=np.array(100*t).flatten()
        full_post[:,idx] = t
    
    return np.median(full_post, axis=1)


    
def make_data_dictionary(row, window=30):
    '''
    This function takes a dataframe with a user_id column and a date column 
    and returns a dictionary with the data in the format required by Stan.   
    '''
    df = pd.read_csv(row['raw_data_file'])
    
    start = np.where(df['SU_banned']==True)[0].min()
    df = df.iloc[start-window:start+window]
    
    #Make a dictionary from a dataframe for a user
    #Stan (our model fitting software) needs a dictionary
    return dict(N=df.shape[0], iter=3,
                            y=df[row['outcome']].values.astype('int'),
                            x=(np.arange(df.shape[0])-window)/window,
                            step = np.arange(df.shape[0]),
                            banned=df['SU_banned'].values.astype('int'))
    

   
   
def process_row(row, model_code, window=30):
    '''
    This function takes a row from the processing_df and fits a model to the data
    '''
    posterior_location = row['posterior_location']

    if not os.path.exists(posterior_location):
        pd.read_csv(row['raw_data_file'])
        
    
        start_time  = time.time()
        data_dict = make_data_dictionary(row, window)
        model = stan.build(model_code, data=data_dict)
        fit = model.sample()
        idata = make_idata(fit, model, data_dict)
        idata.to_json(posterior_location)
            
        print('-'*50)
        print('Completed: ',row['suspended_user'],row['users'])
        print('Posterior saved to: ',posterior_location)
        print('Time elapsed: ',time.time()-start_time)
        print('Total processed  ', glob.glob('./output/posteriors/*').__len__())
        print('-'*50)
        return True
    else:
        return True
    