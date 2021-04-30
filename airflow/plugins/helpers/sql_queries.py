class SqlQueries:

    user_table_insert = ("""
        select 
                    fc.country_code ,
                    country_name,
                    time_year,
                    indicator_code,
                    di.name ,
                    value
                from fact_score_staging fc
                left join dim_country dc on dc.country_code = fc.country_code
                left join dim_indicator di on di.code = fc.indicator_code
                where indicator_code is not null
    """)

    