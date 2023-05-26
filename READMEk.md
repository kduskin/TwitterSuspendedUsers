Step one is to identify the suspended users you want to work with and create suspended_users.csv that has at minimum columns user_id, user_screen_name, user_followers_count and suspension_date.

The next step is to get all of the instances of retweets of each suspended user from our coronavirus and vaccines databases. 

From those retweets, identify the highly engaged followers and moderately engaged followers for each suspended user

Next, get all of the twitter activity for the HE and ME accounts by getting all their tweets from the Twitter API and augmenting it with tweets from our coronavirus and vaccines databases

