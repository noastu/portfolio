update reddit_cities
        set subreddit = z_reddit_cities.subreddit,
        title = z_reddit_cities.title, 
        selftext = z_reddit_cities.selftext,
        upvote_ratio  = z_reddit_cities.upvote_ratio,
        ups = z_reddit_cities.ups,
        downs = z_reddit_cities.downs,
        score = z_reddit_cities.score,
        link_flair_css_class  = z_reddit_cities.link_flair_css_class,
        created_utc = to_timestamp(z_reddit_cities.created_utc, 'YYYY-MM-DD THH24:MI:SSZ'),
        over_18 = z_reddit_cities.over_18,
        id = z_reddit_cities.id,
        kind = z_reddit_cities.kind,
        batch_date = z_reddit_cities.batch_date
        from z_reddit_cities
        where reddit_cities.id = z_reddit_cities.id;

    insert into reddit_cities as tgt
    select src.subreddit  
            ,src.title 
            ,src.selftext 
            ,src.upvote_ratio  
            ,src.ups 
            ,src.downs 
            ,src.score
            ,src.link_flair_css_class  
            ,to_timestamp(src.created_utc, 'YYYY-MM-DD THH24:MI:SSZ') 
            ,src.over_18 
            ,src.id 
            ,src.kind 
            ,src.batch_date
    from z_reddit_cities src
    left outer join reddit_cities tgt on src.id = tgt.id 
    where tgt.id is null;
    
    truncate table z_reddit_cities;