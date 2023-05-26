import argparse
import json
import logging
import os
import requests
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError
import sys
import time
import traceback
from glob import glob
from threading import Thread
from queue import Queue


logger = logging.getLogger(__name__)


def fetch(bearer_token, account_id, starting, stopping, next_token=None,max_results=500,timeout=300):
    query = "from:{}".format(account_id)

    try:
        params = {
            # query must not be greater than 1024 characters FYI
            "query": query,

            # time box to these times, exclusive (ISO8601, "YYYY-MM-DDTHH:mm:ssZ")
            # example timestamp: "2020-01-01T00:00:00Z"
            "start_time": starting,
            "end_time": stopping,

            # limit to ten results (minimum 10, maximum 500)
            # this limit is exclusive (i.e. 10 gets you 9)
            "max_results": max_results,

            # "expand" every field possible. this takes id numbers that appear in a
            # tweet and turns them in actual readable text.
            "expansions": "author_id,referenced_tweets.id,in_reply_to_user_id,geo.place_id,entities.mentions.username,referenced_tweets.id.author_id",

            # fill out these fields
            "user.fields": "created_at,description,entities,id,location,name,protected,public_metrics,url,username,verified,withheld",
            "tweet.fields": "attachments,author_id,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,source,text,withheld",
            "place.fields": "contained_within,country,country_code,full_name,geo,id,name,place_type",
        }

        if next_token is not None:
            params["next_token"] = next_token

        headers = {"Authorization": "Bearer {}".format(bearer_token)}
        
        try:
            r = requests.get("https://api.twitter.com/2/tweets/search/all", params=params, headers=headers,timeout=timeout)
        except Timeout:
            logger.error(f"Request Timed Out ({timeout} Second Limit)")
            return
        except ConnectionError as e:
            logger.error(f"Connection Error\n{e}")
            return 
        except ChunkedEncodingError as e:
            logger.error(f"Connection Error\n{e}")

        if r.status_code >= 500:
            logger.error("received internal server error ({}) from Twitter API".format(r.status_code))
            time.sleep(10)
            return

        if r.status_code == 400:
            raise RuntimeError("your search query was invalid: {}".format(r.text))

        try:
            requests_remaining = int(r.headers.get("x-rate-limit-remaining"))
            seconds_remaining  = int(r.headers.get("x-rate-limit-reset")) - int(time.time())
            # logger.info("api status: requests remaining = {}, seconds remaining = {}".format(requests_remaining, seconds_remaining))
        except (TypeError, ValueError) as e:
            logger.warning("error processing rate limit values: {}".format(e))
            logger.warning(r.headers)
            logger.warning(r.text)
            time.sleep(10)
            return

        if r.status_code == 429:
            logger.error("reached rate limit, sleeping for {} seconds".format(seconds_remaining))
            time.sleep(seconds_remaining + 1)
            return
        else:
            time.sleep(1)

        return r.json()
    except json.decoder.JSONDecodeError as e:
        logger.error(str(e))
        logger.error(traceback.format_exc())


def parse(raw, file_path):
    users = {}  # keyed by user id
    for user in raw.get("includes", {}).get("users", []):
        user_id = user["id"]
        users[user_id] = user

    linked_tweets = {}  # keyed by tweet id
    for tweet in raw.get("includes", {}).get("tweets", []):
        tweet_id = tweet["id"]
        linked_tweets[tweet_id] = tweet

    with open(file_path, "at") as f:
        for tweet in raw.get("data", []) + list(linked_tweets.values()):
            author = users.get(tweet["author_id"])

            obj = {
                "id": tweet["id"],
                "conversation_id": tweet["conversation_id"],
                "created_at": tweet["created_at"],
                "tweet": tweet["text"],
                "hashtags": [x["tag"] for x in tweet.get("entities", {}).get("hashtags", [])],
                "urls": [x["expanded_url"] for x in tweet.get("entities", {}).get("urls", [])],
                "source": tweet.get("source", None),
                "language": tweet["lang"],
                "retweet_count": tweet["public_metrics"]["retweet_count"],
                "reply_count": tweet["public_metrics"]["reply_count"],
                "like_count": tweet["public_metrics"]["like_count"],
                "quote_count": tweet["public_metrics"]["quote_count"],
                "in_reply_to_user_id": tweet.get("in_reply_to_user_id", None),

                "user_id": tweet["author_id"],
                "user_screen_name": author["username"],
                "user_name": author["name"],
                "user_description": author["description"],
                "user_location": author.get("location"),
                "user_created_at": author["created_at"],
                "user_followers_count": author["public_metrics"]["followers_count"],
                "user_friends_count": author["public_metrics"]["following_count"],
                "user_statuses_count": author["public_metrics"]["tweet_count"],
                "user_verified": author["verified"],

                "referenced_tweets": tweet.get("referenced_tweets"),
            }
            print(json.dumps(obj), file=f)

    return len(raw.get("data", []))

def fetch_from_list(**kwargs): 
    logging.basicConfig(format="%(asctime)s %(levelname)-8s - %(message)s", level=logging.INFO)
    
    # # see what we have loaded already
    loaded = [os.path.split(x)[1].split(".", -1)[0] for x in glob(os.path.join(kwargs["output"], "*")) if (x.endswith(".json"))]
    
    # load credentials
    credentials = []
    with open(kwargs["credentials"], "rt") as f:
        credentials = json.load(f)
    logger.info("found {} credentials to use".format(len(credentials)))


    # get the list of ids
    accounts = Queue()
    for line in kwargs["account_list"]:
        account_id = line.strip().strip('"')
        
        # find account ids that we've already loaded and do not load them again
        if account_id not in loaded:
            accounts.put(account_id)
        else:
            logger.info("already loaded {}".format(account_id))
            
    threads = []
    total_accounts=accounts.qsize()
    for credential in credentials:
        t = Thread(args=(accounts, credential["bearer_token"], total_accounts, kwargs), 
                   target=process_account)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
        
def process_account(queue, bearer_token, total_accounts, kwargs):
    logger = logging.getLogger()
    
    while not queue.empty():
        processed = total_accounts-queue.qsize()
        if processed % 500==0:
            logger.info(f"{processed}/{total_accounts} accounts processed")
        account_id = queue.get(block=False)
        if account_id is None:
            continue
            
        logger.info("fetching tweets for {} with {}".format(account_id,bearer_token[-5:]))
        pages = 0
        total = 0
        next_token = None
        retries = 0
        max_results = 500
        while True:
            results = fetch(bearer_token, account_id, kwargs["starting"], kwargs["stopping"], next_token,max_results)
            if results is None:
                retries+=1
                if retries >=20 and retries <40:
                    #decrease max results 
                    max_results= 250
                elif retries >=40 and retries <60:
                    #decrease max results and increase wait time 
                    time.sleep(10)
                    max_results=100
                elif retries >= 60 and retries <80:
                    #decrease max results and increase wait time
                    time.sleep(30)
                    max_results=50
                elif retries >=80:
                    #decrease max results 
                    max_results=10
                logger.info(f"Retry {retries} for account {account_id}, max_results: {max_results}")    
                continue  # try the page again

            try:
                total = total + parse(results, "{}/{}.json".format(kwargs["output"], account_id))

                # is there more data?
                if "meta" in results and results["meta"].get("next_token"):
                    next_token = results["meta"]["next_token"]
                    pages = pages + 1
                else:
                    logger.info("finished fetching {} tweets for {}".format(total, account_id))
                    break  # break the loop and go to the next account id
            except Exception as e:
                logger.error("GENERAL EXCEPTION: {}".format(e))
                logger.error(traceback.format_exc())

def main(**kwargs):
    # load credentials
    bearer_token = None
    with open(kwargs["credentials"], "rt") as f:
        credentials = json.load(f)
        bearer_token = credentials.get("bearer_token")
        if bearer_token is None:
            raise RuntimeError("could not load bearer token from credentials.json")

    # see what we have loaded already
    loaded = [os.path.split(x)[1].split(".", -1)[0] for x in glob(os.path.join(kwargs["output"], "*")) if (x.endswith(".json"))]

    # get the list of ids
    account_ids = []
    for line in sys.stdin:
        account_id = line.strip().strip('"')

        # find account ids that we've already loaded and do not load them again
        if account_id not in loaded:
            account_ids.append(account_id)
        else:
            logger.info("already loaded {}".format(account_id))

    for account_id in account_ids:
        logger.info("fetching tweets for {}".format(account_id))
        pages = 0
        total = 0
        next_token = None
        while True:
            results = fetch(bearer_token, account_id, kwargs["starting"], kwargs["stopping"], next_token)
            if results is None:
                continue  # try the page again

            try:
                total = total + parse(results, "{}/{}.json".format(kwargs["output"], account_id))

                # is there more data?
                if "meta" in results and results["meta"].get("next_token"):
                    next_token = results["meta"]["next_token"]
                    pages = pages + 1
                else:
                    logger.info("finished fetching {} tweets for {}".format(total, account_id))
                    break  # break the loop and go to the next account id
            except Exception as e:
                logger.error("GENERAL EXCEPTION: {}".format(e))
                logger.error(traceback.format_exc())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="fetch_user_tweets",
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__,
    )
    parser.add_argument("-c", "--credentials", default="academic_credentials.json", help="path to a credentials file")
    parser.add_argument("--output", required=True, help="path to directory where outputs will be written")
    parser.add_argument("--starting", required=True, help="the time to start the search (YYYY-MM-DDTHH:mm:ssZ)")
    parser.add_argument("--stopping", required=True, help="the time to stop the search (YYYY-MM-DDTHH:mm:ssZ)")
    parser.add_argument("--credentials", required=True, help="the json file containing bearer token")
    # parser.add_argument("--include_refs", required=True, help="whether or not to return items referenced in the tweet") 
    args = parser.parse_args()

    # configure a basic logger
    logging.basicConfig(format="%(asctime)s %(levelname)-8s - %(message)s", level=logging.INFO)

    try:
        main(**vars(args))
    except Exception as e:
        logger.error(str(e))
        logger.error(traceback.print_exc())
