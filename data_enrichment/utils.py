

class Scoring:

    @staticmethod
    def get_query(is_test):
        schema = "omh_schema_test" if is_test else "omh_schema"
        post_hist_with_last_engagement_query = f"""with post_eng as (
        select post_fk, curr_engagement, timestamp 
        from {schema}."PostHistory" 
        ) ,
        post_max_ts as(
            select post_fk, max(timestamp) as max_timestamp
            from {schema}."PostHistory" ph 
            group by 1
        )
        select 	t1.post_fk as post_id, curr_engagement as post_engagement
        from post_eng t1 inner join post_max_ts t2
        ON t1.post_fk = t2.post_fk AND t1.timestamp = t2.max_timestamp
        ;
        """
        return post_hist_with_last_engagement_query

    MIN_HOURS = 3