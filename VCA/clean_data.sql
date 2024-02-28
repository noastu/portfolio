select breed,
    details, 
    CASE WHEN len(regexp_extract_all(stats.lifespan, '\d+', 0)) = 2 THEN CAST(regexp_extract_all(stats.lifespan, '\d+', 0)[1] AS INT32)
        ELSE 0 END as lifespan_lower_yr,
    CASE WHEN len(regexp_extract_all(stats.lifespan, '\d+', 0)) = 2 THEN CAST(regexp_extract_all(stats.lifespan, '\d+', 0)[2] AS INT32)
        ELSE 0 END as lifespan_upper_yr,
    CASE WHEN len(regexp_extract_all(stats.height, '\d+', 0)) = 2 THEN CAST(regexp_extract_all(stats.height, '\d+', 0)[1] AS INT32)
        ELSE 0 END as height_lower_in,
    CASE WHEN len(regexp_extract_all(stats.height, '\d+', 0)) = 2 THEN CAST(regexp_extract_all(stats.height, '\d+', 0)[2] AS INT32)
        ELSE 0 END as height_upper_in,
    CASE WHEN len(regexp_extract_all(stats.weight, '\d+', 0)) = 2 THEN CAST(regexp_extract_all(stats.weight, '\d+', 0)[1] AS INT32)
        ELSE 0 END as weight_lower_lb,
    CASE WHEN len(regexp_extract_all(stats.weight, '\d+', 0)) = 2 THEN CAST(regexp_extract_all(stats.weight, '\d+', 0)[2] AS INT32)
        ELSE 0 END as weight_upper_lb,
    CASE WHEN len(regexp_extract_all(traits."Jogging Partner", '\d+', 0)) = 2 THEN CAST(regexp_extract_all(traits."Jogging Partner", '\d+', 0)[1] AS INT32)
        ELSE 0 END as jogging_partner,
    CASE WHEN len(regexp_extract_all(traits."Lap Dog", '\d+', 0)) = 2 THEN CAST(regexp_extract_all(traits."Lap Dog", '\d+', 0)[1] AS INT32)
        ELSE 0 END as lap_dog,
    CASE WHEN len(regexp_extract_all(traits."Good With Children", '\d+', 0)) = 2 THEN CAST(regexp_extract_all(traits."Good With Children", '\d+', 0)[1] AS INT32)
        ELSE 0 END as good_with_children,
    CASE WHEN len(regexp_extract_all(traits."Warm Weather", '\d+', 0)) = 2 THEN CAST(regexp_extract_all(traits."Warm Weather", '\d+', 0)[1] AS INT32)
        ELSE 0 END as warm_weather,
    CASE WHEN len(regexp_extract_all(traits."Cold Weather", '\d+', 0)) = 2 THEN CAST(regexp_extract_all(traits."Cold Weather", '\d+', 0)[1] AS INT32)
        ELSE 0 END as cold_weather,
    CASE WHEN len(regexp_extract_all(traits."Grooming Requirements", '\d+', 0)) = 2 THEN CAST(regexp_extract_all(traits."Grooming Requirements", '\d+', 0)[1] AS INT32)
        ELSE 0 END as grooming_requirements,
    CASE WHEN len(regexp_extract_all(traits."Shedding", '\d+', 0)) = 2 THEN CAST(regexp_extract_all(traits."Shedding", '\d+', 0)[1] AS INT32)
        ELSE 0 END as shedding,
    CASE WHEN len(regexp_extract_all(traits."Barking", '\d+', 0)) = 2 THEN CAST(regexp_extract_all(traits."Barking", '\d+', 0)[1] AS INT32)
        ELSE 0 END as barking,
    CASE WHEN len(regexp_extract_all(traits."Ease Of Training", '\d+', 0)) = 2 THEN CAST(regexp_extract_all(traits."Ease Of Training", '\d+', 0)[1] AS INT32)
        ELSE 0 END as ease_of_training,
    strftime(created_ts, '%c') created_ts,
    strftime(get_current_timestamp(), '%c') cleaned_ts
    from vca