import numpy as np
import arviz as az
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import nest_asyncio
nest_asyncio.apply()
import stan
from os.path import exists
import pickle
import os
import json
import pickle


red =[229/255.0,80/255.0,61/255.0, ]
yellow =np.array([243,213,74])/255
orange = np.array([200,150,100])/255
grey = np.array([47,47,56])/255

def plot_posteriors(idata,
                   ylabel='Posts per day',
                   xlabel='Day',
                   window=30,
                   axs=False):

    if axs==False:
        sns.set_style('white')
        sns.set_context('paper', font_scale=1.25)
        plt.figure(figsize=(6,4))
        ax = plt.gca()
    else:
        ax = axs
    plt.sca(ax)

    #We can extract it (it's in log scale, so we take an exp)
    #It is a matrix of 4000 x window*2, containing samples from the posterior predictive distribution
    exp_post = np.exp(idata.posterior_predictive.exp_hat)

    #And plot various credible intervals, perhaps with shading.
    for q in (1,3,6,11, 25,50,75):
        cis = np.nanpercentile(np.array(exp_post).reshape(4000,window*2), q=[0+q/2, 100-q/2],axis=0)
        plt.fill_between(np.arange(window*2), cis[0], cis[1], alpha = q/100,color= orange)

    #Our idata object has actual data, let's plot that
    #Stored as seen below
    plt.scatter(np.arange(window*2), idata.observed_data.y,color=np.array([47,47,56])/255,zorder=2,s=10)


    #exp_hat contains the mean function
    ci = np.nanpercentile(np.exp(np.array(idata.posterior_predictive.exp_hat)).reshape(4000,window*2),
                   axis=0,
                   q=[5.5,50, 94.5])

    #Plot the 94\% credible interval as being shaded
    plt.fill_between(np.arange(window*2), ci[0], ci[2], alpha=.35, color='grey')

    #Plot the median as a line
    plt.plot(np.arange(window*2), ci[1], color=np.array([47,47,56])/255)

    #Clean things up
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.ylim(0,np.max(idata.observed_data.y)*1.5)
    plt.xlim(0,window*2)
    plt.plot([window-1,window-1],
             [0, 4000],
             color=np.array([47,47,56])/255,
             ls='--')
    plt.tight_layout()

def plot_posterior_and_change(posterior_location,
                   ylabel='Posts per day',
                   xlabel='Day',
                   window=30,
                   axs=False):
    idata = az.from_json(posterior_location)
    fig, ax = plt.subplots(2,1)
    #plot posterior 
    plot_posteriors(idata, ylabel=ylabel, xlabel=xlabel, window=window, axs=ax[0])
    
    #Plot change
    t=idata.posterior_predictive.mu_hat_without_ban[:,:,-1]-idata.posterior_predictive.mu_hat[:,:,-1] 
    t/=idata.posterior_predictive.mu_hat_without_ban[:,:,-1]
    plt.sca(ax[1])
    sns.distplot(100*np.array(t).flatten(),kde=True, ax=ax[1])
    plt.savefig('./output/figures/posteriors/'+posterior_location.split('/')[-1].split('.')[0]+'.png',dpi=300)
    
    
def plot_change_by_group(group, ax, color,xlab=False):
    follower_counts = json.load(open('./dat/user_followers_count.json','rb'))
    group['followers'] = [np.round(follower_counts[user]/1000).astype('int') for user in group['suspended_user']]
    for idx, percentile in enumerate([97, 89, 75, 50]):
        for row in group.iterrows():
            plt.plot([row[1]['followers'], row[1]['followers']],
                    np.nanpercentile(row[1]['results'], [(100-percentile)/2, 100-(100-percentile)/2]), 
                    color=color, solid_capstyle='projecting',
                        lw=10,alpha=.25,zorder=idx)
    for percentile in [3,11,25,50,75,89,97]:
        group.loc[:,str(percentile) + '%'] = group['results'].apply(lambda x: np.nanpercentile(x, percentile)) 
    
    plt.scatter(group['followers'], group['results'].apply(np.nanmedian),s=10,color='k',zorder=idx+1)
    xlim = plt.gca().get_xlim()
    plt.plot(xlim, [0,0], color=grey, ls='--')
    plt.xlim(xlim)

     
    new_row = group.iloc[0].copy()
    new_row['suspended_user'] = 'Average'
    new_row['follower_counts'] = 0
    print(new_row)
    for percentile in [3,11,25,50,75,89,97]:
        new_row[str(percentile) + '%'] = np.percentile(np.mean(group['results'].apply(np.array)),percentile) 
    
    group = pd.concat([group, pd.DataFrame(new_row).T])
        
    plt.ylim(-200,200)
    if xlab:
        plt.xlabel('Followers (thousands)')
    plt.ylabel("Change in posting (%)")
    
    
    return group