
create or replace function get_user_info(userStatus_values omh_schema."VerifiedStatus"[])
returns table(volunteer_id text, liked_posts int, reported_posts int, sared_posts int, commented_posts int,copied_posts int, deleted_posts int)
as $$
begin
	create temporary table approved_users_action (
	    volunteer_id text,
	    post_id text,
	    action_type omh_schema."ActionType"
	);
	
	insert into approved_users_action (volunteer_id, post_id, action_type)
	select v.volunteer_id,a.post_id, a.action_type
	from "Volunteer" v 
	inner join "Action" a 
		on v.volunteer_id = a.volunteer_id 
	where v.verified = any(userStatus_values)--'APPROVED'::omh_schema."VerifiedStatus"
	;
	
	--get action counts for user
	return query
		select au.volunteer_id, 
			count(case when au.action_type = 'LIKE'::omh_schema."ActionType" then 1 end) as liked_posts,
			count(case when au.action_type = 'REPORT'::omh_schema."ActionType" then 1 end) as reported_posts,
			count(case when au.action_type = 'SHARE'::omh_schema."ActionType" then 1 end) as sared_posts,
			count(case when au.action_type = 'COMMENT'::omh_schema."ActionType" then 1 end) as commented_posts,
			count(case when au.action_type = 'COPY'::omh_schema."ActionType" then 1 end) as copied_posts,
			count(case when p.publish_date< p.removed_date then 1 end) as deleted_posts
		from approved_users_action au
		inner join "Post" p 
			on au.post_id = p.post_id
		group by au.volunteer_id
		;
	
	--get info by time?
end;
$$
LANGUAGE plpgsql;

	

