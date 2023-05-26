import numpy as np
import arviz as az
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import nest_asyncio
import stan
from os.path import exists
import pickle
import os
import json
import pickle

#Load model code
with open('./src/model.stan', 'r') as file:
    model_code = file.read()   

    
def process_one(one_user, window, outcome, indicator, folder='/', user_id='none'):
    
    if (np.sum(one_user[indicator]==True) > (window-1)) :
        print('./output/posteriors'+ folder + str(user_id) + '.json')

        stan_data = make_data_dictionary(one_user,
                         outcome=outcome,
                         indicator=indicator,
                         window=window)
        model, fit = get_fit(stan_data, model_code)
        idata = make_idata(fit, model, stan_data)
        idata.to_json('./output/posteriors'+ folder + str(user_id) + '.json')
        return True
    else: 
        return False
    

def get_change(posterior,adj=True):
    
    idata = az.from_json(posterior)
    
    with_ban =  np.array(np.exp(idata.posterior_predictive.mu_hat))
    without_ban =  np.array(np.exp(idata.posterior_predictive.mu_hat_without_ban))

    return with_ban.reshape(4000,60),  without_ban.reshape(4000,60)



def make_idata(fit, model, stan_data):
    '''
        This function takes a fit object and returns an ArviZ idata object
    '''
    idata = az.from_pystan(
        posterior=fit,
        posterior_predictive=['y_hat','exp_hat',
                              'mu_hat',
                              'y_without_ban',
                              'mu_hat_without_ban',
                              'exp_hat_without_ban'],
        observed_data=['y'],
        log_likelihood={"y": "log_lik"},
        posterior_model = model,
        coords={"timestep": np.arange(stan_data["N"])},
        dims={
            "theta": ["timestep"],
            "y": ["timestep"],
            "log_lik": ["timestep"],
            "y_hat": ["timestep"],
            },
        )
    #Takes the output from Stan and moves it to the ArViZ format
    return idata