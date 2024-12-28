from DB_Manager_EP.db_table_objects import Post
from table_objects import post
from utils import Scoring

from json import dumps, loads
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import pytz


def run(event):
    try:
        print('READ DATA FROM DB')
        # initialize post object
        post_obj = post.PostHandler(event)

        # read all post from db
        all_posts = query_post(post_obj)

        # read post_current_engagement
        post_with_engagement = post_obj.db_obj.run_select_query(Scoring.get_query(event['test_env_status']))

        # merge engagement with posts
        all_posts = pd.merge(all_posts, post_with_engagement, on='post_id', how='left')

        print("START SCORING")
        # run the score function
        scored_posts = score_all(all_posts)

        print("UPDATE DB")
        # update db
        post_obj.db_obj.update_table(Post.__tablename__, scored_posts[['post_id', 'virality_score']], 'post_id', 'virality_score')

        return "success"
    except Exception as e:
        print("AN ERROR HAS OCCURRED", e)
        return "error"


def query_post(post_obj_):
    tbl_result = post_obj_.db_obj.query_table_orm(Post, to_df=True)
    if tbl_result[0] is None:
        return None
    else:
        return tbl_result[0]


def sentiment_adjustment(x):
    if x in ['5.0', '1.0', '1', '0', '0.0']:
        return True
    else:
        return False


def score_all(posts: pd.DataFrame):
    # reset_current_scrape_results()
    # set_scraped_start_time()
    posts['pro_israel'] = posts['sentiment'].apply(sentiment_adjustment)
    posts['created_at'] = pd.to_datetime(posts['created_at'])
    posts['created_at'] = pd.to_datetime(posts['created_at'], errors='coerce')

    posts['is_new'] = posts['created_at'].apply(lambda x: is_within_last_n_hours_vectorized(x, hours=3))

    posts = posts[['post_id', 'platform_type', 'url', 'sentiment', 'pro_israel', 'created_at', 'post_engagement', 'is_new']]
    scored_posts = posts.apply(
        lambda row: score_a_post(row), axis=1)
    scored_posts.sort_values(['virality_score'], inplace=True, ascending=False)

    return scored_posts


def score_a_post(post):
    """
    Calculate a virality score for a post based on given metrics.
    Returns:
    - post updated with virality_score and if the creator of the post is influencer.
    """
    support_israel = post['pro_israel']
    eng_ = post['post_engagement']
    is_new = post['is_new']
    post, virality_score, weighted_sum = score_by_date(post,
                                                       eng_,
                                                       is_new
                                                       # support_israel
                                                       )
    # virality_score_countries, weighted_sum = score_by_location(post['city'], countries, weighted_sum)
    # virality_score += virality_score_countries
    # virality_score += sum([multiply_with_weights(post[k + '_scaled'], v) for k, v in WEIGHTS.items()])
    # virality_score = divide(virality_score, weighted_sum)
    post['virality_score'] = virality_score
    return post


def score_by_date(post, eng_, is_new):
    virality_score = 0
    virality_score += np.sqrt(eng_)
    if is_new:
        virality_score += 2
    else:
        virality_score -= 2

    return post, virality_score, eng_


def is_within_last_n_hours_vectorized(created_at,  hours=3):
    """
    Checks if datetimes in a DataFrame column are within the last N hours.

    Args:
        df (pd.DataFrame): The DataFrame.
        datetime_column (str): The name of the datetime column.
        hours (int): The number of hours to check against.

    Returns:
        pd.Series: A boolean Series indicating whether each datetime is within the last N hours.
        Returns Series of False if datetime_column is None.

    Raises:
        TypeError: if datetime_column is not str.
        KeyError: If the datetime column is not found in the DataFrame.
        ValueError: if the datetime column is not datetime64[ns, UTC]
    """

    est = pytz.timezone('US/Eastern')
    curr_timestamp_utc = datetime.now(est).astimezone(pytz.utc)

    if pd.isna(created_at):
        return False

    if created_at.tz is None:  # Check if tz-naive
        created_at_utc = created_at.tz_localize('UTC')  # Localize to UTC
    else:
        created_at_utc = created_at.astimezone(pytz.utc)

    time_difference = curr_timestamp_utc - created_at_utc
    return time_difference <= pd.Timedelta(hours=hours)

run({
    "id": "2024-09-22 07:48:23.650"
    ,'event_name': 'scooper'
    , 'test_env_status': True
    , 'bucket_name': 'data-omhds'
    , 'input_file': 'test/scooper_imports/2024/09/12/_scooper.json'
}
)