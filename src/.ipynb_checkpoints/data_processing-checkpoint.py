import argparse
from glob import glob
import pandas as pd 
import csv
import re
import os

def get_tweet_type(ref):
    if type(ref)==list:
        return ref[0]['type']
    
def get_parent_id(ref):
    #return a string of the parent id
    if type(ref)==list:
        return ref[0]['id']
    
def is_retweet(ref):
    if type(ref) != list:
        return False
    if ref[0]["type"] == 'retweeted':
        return True
    else:
        return False
    
def create_retweeted_status(ref):
    if type(ref) != list:
        return ref
    elif ref[0]["type"]=="retweeted":
        return ref[0]["id"]
    
def create_retweeted_status_user_screen_name(row):
    return get_retweeted_user(row.tweet)
    
def get_retweeted_user(tweet):
    match = re.match("RT @\S*",tweet)
    if match:
        m = match.group(0)
        return m.split("@")[1].replace(":","")
    
def get_full_text(row):
    #if the tweet is an original, or a quote, keep it how it is
    #if the tweet is a retweet, get the orignal (retweeted) tweet, because the TW api returns truncated versions of retweets
    #if the tweet is a reply, concatenate the original (replied to) tweet and the reply so that keyword searching will search both 
    if row.tweet_type=="retweeted" and type(row.original_text)==str:
        return row.original_text
    elif (row.tweet_type=="replied_to" or row.tweet_type=="quoted") and type(row.original_text)==str:
        return row.tweet +" " + row.original_text
    else:
        return row.tweet
    
def get_account_activity_database(su_name,suspension_date,database_data_files):
    #load data from both coronavirus database and vaccine database
    activity=pd.concat([pd.read_csv(f,
                                    dtype={
                                    "id": str,
                                    "user_id": str,
                                    "retweeted_status_user_id": str,
                                    "quoted_status_user_id": str,
                                    "in_reply_to_status_user_id": str},low_memory=False) for f in database_data_files]
                      )
    #drop duplicate tweets that got picked up in both databases
    activity = activity.drop_duplicates("id")
    #filter to the month before and month after user suspension
    activity.created_at = pd.to_datetime(activity.created_at)
    activity = activity[(activity.created_at > suspension_date - pd.Timedelta("32 d")) & 
                        (activity.created_at < suspension_date + pd.Timedelta("32 d"))]
    return activity

def get_account_activity_api(su_name,suspension_date,user_list, twitter_data_files):
    api_tweets_list = []
    for f in twitter_data_files:
        t = pd.read_json(f,
                         lines=True,
                         dtype={"user_id": str,
                                 "id":str,
                                 "retweeted_status_user_id": str,
                                 "quoted_status_user_id": str,
                                 "in_reply_to_status_user_id": str})
        api_tweets_list.append(t)

    api_tweets = pd.concat(api_tweets_list)
    
    #create dictionary that connects tweet ids to tweet text
    tweet_id_to_text = dict(zip(api_tweets.id,api_tweets.tweet))
    
    ###reconstruct the original tweet text 
    ###because retweets are truncated by the twitter api and we want to connect replies to what they are replying to
    print("Recovering Original Text...")
    api_tweets["tweet_type"] = api_tweets.references.apply(get_tweet_type)
    api_tweets["tweet_type"] = api_tweets.tweet_type.fillna("tweet")
    api_tweets["parent_id"] = api_tweets.references.apply(get_parent_id)

    #create field for original text (text copied from the parent tweet of either a retweet, quote, or reply)
    api_tweets["original_text"] = api_tweets.parent_id.map(tweet_id_to_text)

    #now that we have used the extra tweets returned by the twitter API we can cut the data down to only the relevant time period
    api_tweets = api_tweets[(api_tweets.created_at > suspension_date - pd.Timedelta("32 d")) & 
                            (api_tweets.created_at < suspension_date + pd.Timedelta("32 d"))]

    #get tweets by the reliable amplifiers only (TW API returns parent tweets by other users) 
    api_ra_tweets = api_tweets[api_tweets.user_id.isin(user_list)]
    api_ra_tweets["is_retweet"] = api_ra_tweets.references.apply(is_retweet)
    
    #recreate the fields expected that aren't present in data from the TW API 
    api_ra_tweets["retweeted_status_id"] = api_ra_tweets.references.apply(create_retweeted_status)
    api_ra_tweets["retweeted_status_user_screen_name"]=api_ra_tweets.apply(create_retweeted_status_user_screen_name,axis=1)
    
    #deal with missing users (those present in our database but not from the twitter API) 
    missing_users = set(user_list).difference(api_ra_tweets.user_id)    
    return api_ra_tweets, missing_users
    
def load_data(su_name, suspended_user_meta, covid_keyword_file, database_data_files, twitter_data_files):   
    #get the suspension date 
    suspension_date = pd.to_datetime(suspended_user_meta[suspended_user_meta.user_screen_name==su_name].suspension_date.iloc[0])
    
    #load in covid keywords 
    covid_keywords = open(covid_keyword_file, "r").read().split("\n")
    
    #load in data from the internal databases 
    print("Loading database tweets...")
    db_tweets = get_account_activity_database(su_name,
                                              suspension_date,
                                              database_data_files)
    db_tweets["is_retweet"] = ~db_tweets.retweeted_status_id.isna()
    
    #Load the account activity data from the twitter API
    print("Loading Twitter API tweets...")
    api_tweets, missing_users = get_account_activity_api(su_name, 
                                                         suspension_date, 
                                                         db_tweets.user_id.unique(), 
                                                         twitter_data_files)
    
    missing_users = dict((x,db_tweets[db_tweets.user_id==x].user_screen_name.iloc[0]) for x in missing_users)
    print("Missing Accounts")
    print(missing_users) 
    
    #create dataframe that is all reliable amplifier tweets --> merge the tweets from the database with the tweets from twitter (and then make sure to deduplicate)
    #prioritize keeping the tweets from the twitter api if possible since they have the original text associated 
    ##(might need to come back to this later??) 
    all_tweets = pd.concat([api_tweets,db_tweets]).drop_duplicates("id",keep="first")

    #drop missing suspended accounts
    all_tweets = all_tweets[~all_tweets.user_id.isin(missing_users.keys())]
    
    #set variable if the tweet shows up in our database
    all_tweets["in_covid_db"] = all_tweets.id.isin(db_tweets.id.unique())
    
    #set up full text: the tweet, or the original tweet if its a retweet (because the tw api returns a truncated version of retweets)
    #or if it is a reply or quote, the text from the original tweet concatenated with the reply/quote 
    all_tweets["full_text"] = all_tweets.apply(get_full_text,axis=1)    
    
    #check if tweet (full text) contains covid keyword
    all_tweets["contains_covid_kw"] = all_tweets.full_text.apply(lambda x: any([kw.lower() in x.lower() for kw in covid_keywords]))
    
    #set the "SU_banned" boolean 
    all_tweets.created_at = pd.to_datetime(all_tweets.created_at)
    all_tweets["SU_banned"] = all_tweets.created_at > suspension_date
    
    #set days since suspension to make things easier later
    all_tweets["days_since_suspension"] = (all_tweets.created_at - suspension_date).dt.days
    
    return all_tweets


def get_counts(su_name,suspended_user_meta,data):
    #get the suspension date 
    suspension_date = pd.to_datetime(suspended_user_meta[suspended_user_meta.user_screen_name==su_name].suspension_date.iloc[0])
    
    #daily twitter activity (covid vs non covid, before/after banning)
    daily_tweets = data.groupby(["user_id","contains_covid_kw"]).resample("D",on="created_at").count().id.reset_index().pivot(index=['user_id','created_at'], columns="contains_covid_kw", values='id').fillna(0)
    
    daily_tweets = daily_tweets.reset_index().rename({False:"non_covid_tweets",True:"covid_tweets"},axis=1)
    
    # go through each user and force it to have the full time range 
    date_index = pd.date_range(start=daily_tweets.created_at.min().floor(freq="D"), end=daily_tweets.created_at.max().ceil(freq="D"),freq="D")
    date_df = pd.DataFrame(date_index,columns=["dates"])

    users_list = []
    for user,user_df in daily_tweets.groupby("user_id"):
        t = pd.merge(left=date_df,right=user_df, left_on="dates",right_on="created_at",how="outer")
        t.user_id.fillna(user,inplace=True)
        t.created_at.fillna(t.dates,inplace=True)
        t.fillna(0,inplace=True)
        users_list.append(t)

    daily_tweets = pd.concat(users_list)
    daily_tweets["total_tweets"] = daily_tweets["non_covid_tweets"] + daily_tweets["covid_tweets"]
    daily_tweets["SU_banned"] = False
    daily_tweets.loc[(daily_tweets.created_at >= suspension_date),"SU_banned"] =True
    
    #daily retweet activity (covid vs non covid, retweets of suspended user) 
    retweet_counts = data[(data.is_retweet)].groupby(["user_id","contains_covid_kw"]).resample("D",on="created_at").count().id.reset_index().pivot(index=['user_id','created_at'], columns='contains_covid_kw', values='id').fillna(0)
    
    retweet_counts = retweet_counts.reset_index().rename({False:"non_covid_retweets",True:"covid_retweets"},axis=1)
    
    su_retweets = pd.DataFrame(data[(data.retweeted_status_user_screen_name == su_name) & (data.contains_covid_kw)].groupby("user_id").resample("D",on="created_at").count().id).rename({"id":"su_covid_retweets"},axis=1)
    
    retweet_counts = pd.merge(left=retweet_counts, right=su_retweets, left_on=["user_id","created_at"], right_index=True, how="outer")
    
    daily_tweets = pd.merge(left=daily_tweets, right=retweet_counts, left_on=["user_id","created_at"], right_on=["user_id","created_at"], how="outer").fillna(0)
    
    daily_tweets["days_since_suspension"] = (daily_tweets.created_at - suspension_date).dt.days
    return daily_tweets


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--metadata_file', default='data/suspended_users.csv', help='the csv file with suspended user screen names and suspension dates')
    parser.add_argument('--covid_keyword_file', default='data/updated_covid_keywords.txt', help='path to the .txt file containing covid keywords')
    parser.add_argument('--database_data_dir', help="path to the directory that holds the data from the internal databases")
    parser.add_argument('--api_data_dir',help="path to the directory that hold the data from the Twitter API")
    parser.add_argument('--save_prefix', default='data', help="prefix for the files where data is saved")
    args = parser.parse_args()

    suspended_user_meta = pd.read_csv(args.metadata_file)
    
    for su in suspended_user_meta.user_screen_name:
        print(f"Processing data for {su}....")  
        db_data_files = glob(f"{args.database_data_dir}/{su}*")
        tw_data_files = glob(f"{args.api_data_dir}/*".replace("su",su))
        data = load_data(su,
                         suspended_user_meta,
                         args.covid_keyword_file,
                         db_data_files,
                         tw_data_files)
        f = args.save_prefix+f"{su}_ra_combined_api_db_data.csv"
        print(f)
        data.to_csv(f,
                    index=False,
                    quoting=csv.QUOTE_NONNUMERIC)
        
        print("Data loaded, getting counts...")
        daily_counts = get_counts(su, suspended_user_meta,data)

        f = args.save_prefix + f"{su}_ra_daily_activity_counts.csv"
        print(f"Saving to {f}\n\n")
        daily_counts.to_csv(f,index=False)