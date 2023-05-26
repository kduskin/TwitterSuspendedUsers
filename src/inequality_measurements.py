import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def shannon(counts):
    counts = counts[counts.nonzero()]
    if min(counts) <0:
        raise ValueError("Values must be postive")
    p_i = counts / counts.sum()
    return -(p_i * np.log(p_i)).sum()


def get_retweet_diversity(df,measurement_func,dtype="float"):
    """
    df: dataframe where each row is a tweet, expected columns: retweeted_status_user_id (str)
    measurement_func: function calculating the measurement on the retweeted status ids
    """
    retweet_distribution = df.retweeted_status_user_screen_name.value_counts().values.astype(dtype)
    if len(retweet_distribution) == 0:
        return np.nan
    return measurement_func(retweet_distribution)

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--metadata_file', default='data/suspended_users.csv', help='the csv file with suspended user screen names and suspension dates')
    parser.add_argument('--group')
    parser.add_argument('--save_dir', default='data', help="directory to save the data to")
    args = parser.parse_args()
    
    suspended_user_meta = pd.read_csv(args.metadata_file)
    df_list=[]
    for su in suspended_user_meta.user_screen_name:
        print(su)
        tweets = pd.read_csv(f"data/full_ra_data_with_zeros/{args.group}_{su}_ra_combined_api_db_data.csv",
                             dtype={"id": str,
                                    "user_id": str,
                                    "retweeted_status_user_id": str,
                                    "quoted_status_user_id": str,
                                    "in_reply_to_status_user_id": str},
                            low_memory=False)

        tweets.created_at = pd.to_datetime(tweets.created_at,errors='coerce')
        tweets = tweets[(tweets.days_since_suspension >= -30)&(tweets.days_since_suspension <30)]
        print(tweets.user_id.nunique())
        #calculate measurements for before/after suspension, about covid/all tweets
        for (func, dtype, measurement_name) in [(gini,float,"gini"),(shannon,int,"shannon")]:
            df = pd.DataFrame(columns=["before_suspension","after_suspension","covid_before_suspension","covid_after_suspension"])
            for user, user_df in tweets.groupby("user_id"):
                
                df.loc[user,"before_suspension"] = get_retweet_diversity(user_df[~user_df.SU_banned],func,dtype)
                df.loc[user,"after_suspension"] = get_retweet_diversity(user_df[user_df.SU_banned],func,dtype)        
                df.loc[user,"covid_before_suspension"] = get_retweet_diversity(user_df[(~user_df.SU_banned) & (user_df.contains_covid_kw)],func,dtype)
                df.loc[user,"covid_after_suspension"] = get_retweet_diversity(user_df[(user_df.SU_banned) & (user_df.contains_covid_kw)],func,dtype)
                df.loc[user,"non_covid_before_suspension"] = get_retweet_diversity(user_df[(~user_df.SU_banned) & (~user_df.contains_covid_kw)],func,dtype)
                df.loc[user,"non_covid_after_suspension"] = get_retweet_diversity(user_df[(user_df.SU_banned) & (~user_df.contains_covid_kw)],func,dtype)

            df["suspended_user"] = su
            df["measurement"] = measurement_name
            df = df.reset_index().rename({"index":"user_id"},axis=1)
            print(df.user_id.nunique())
            df_list.append(df)
            
    measurement_df = pd.concat(df_list,ignore_index=True)
    measurement_df.to_csv(f"{args.save_dir}/{args.group}_ra_inequality_measurements.csv")