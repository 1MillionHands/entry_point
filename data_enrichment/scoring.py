from DB_Manager_EP.db_table_objects import Post
from utils import Scoring

from json import dumps, loads
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime


def run(event):
    try:
        # initialize post object
        post_obj = Post(event)

        # read all post from db
        all_posts = post_obj.query_raw_data()

        # read post_current_engagement
        post_with_engagement = post_obj.db_obj.run_select_query(Scoring.post_hist_with_last_engagement_query)

        # merge engagement with posts
        all_posts = pd.merge(all_posts, post_with_engagement, on='post_id', how='left')

        # run the score function
        scored_posts = score_all(all_posts)

        # update db
        post_obj.db_obj.update_table(Post, scored_posts, ['post_id'], scored_posts.virality_score)

        return "success"
    except Exception as e:
        print("AN ERROR HAS OCCURRED", e)
        return "error"

def sentiment_adjustment(x):
    if x in ['5.0', '1.0', '1', '0', '0.0']:
        return True
    else:
        return False


def score_all(posts: pd.DataFrame):
    # reset_current_scrape_results()
    # set_scraped_start_time()
    posts['pro_israel'] = posts['sentiment'].apply(sentiment_adjustment)
    posts = posts[['post_id', 'platform', 'url', 'sentiment', 'pro_israel', 'created_at', 'post_engagement']]
    processed_scraped_posts = posts.apply(
        lambda row: score_a_post(row), axis=1)
    scored_posts = processed_scraped_posts.sort_values(['virality_score'], inplace=True, ascending=False)
    scored_posts = scored_posts.to_dict('records')

    return scored_posts


def score_a_post(post):
    """
    Calculate a virality score for a post based on given metrics.
    Returns:
    - post updated with virality_score and if the creator of the post is influencer.
    """
    support_israel = post['pro_israel']
    eng_ = post['post_engagement']
    post, virality_score, weighted_sum = score_by_date(post,
                                                       eng_
                                                       # support_israel
                                                       )
    # virality_score_countries, weighted_sum = score_by_location(post['city'], countries, weighted_sum)
    # virality_score += virality_score_countries
    # virality_score += sum([multiply_with_weights(post[k + '_scaled'], v) for k, v in WEIGHTS.items()])
    # virality_score = divide(virality_score, weighted_sum)
    post['virality_score'] = virality_score
    return post


def score_by_date(post, eng_):
    virality_score = 0
    virality_score += np.sqrt(eng_)
    if is_new_post(post):
        virality_score += 2
    else:
        virality_score -= 2

    return post, virality_score, eng_


def is_new_post(post, hours=Scoring.MIN_HOURS):
    date_diff = handle_date(post['created_at'])
    return date_diff and date_diff < timedelta(hours=hours)


def handle_date(date_in_post):
    date_diff = None
    created_at = None
    has_time = date_format_include_hours(date_in_post)
    if has_time:
        created_at = datetime.strptime(date_in_post, '%Y-%m-%d %H:%M:%S')
    elif has_time is False:
        created_at = datetime.strptime(date_in_post, '%Y-%m-%d')
    else:
        print("the date is not supported or None - %s", date_in_post)

    if created_at is not None:
        date_diff = (datetime.now() - created_at)
    return date_diff


def date_format_include_hours(date_string):
    try:
        datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
        return True
    except:
        try:
            datetime.strptime(date_string, '%Y-%m-%d')
            return False
        except:
            return None
