-- models/Silver/silver_news.sql

with news_souerce as (
		select 
			* 
		from {{ source('bronze', 'news') }}
	),
	auxiary_souerce as (
		select 
			* 
		from {{ source('bronze', 'auxiliary_table_tck_name') }}
	),
	news_type as (
		select 
			cast(hash as varchar) as hash_news,
			cast(company as varchar) as company,
			cast(title as varchar) as news_titel,
			cast(description as varchar) as news_description,
			cast(url as varchar) as news_url,
			cast(sentiment as varchar) as news_sentiment,
			cast(published_at as timestamp) as published_at
		from news_souerce
	),
	auxiary_type as (
		select
			cast(ticket as varchar) as hash,
			upper(cast(ticket as varchar)) as ticket,
			cast(company as varchar) as company
		from auxiary_souerce
	),
	news_with_ticket as (
		select 
			a.hash_news,
			a.company,
			b.ticket,
			a.news_titel,
			a.news_description,
			a.news_url,
			a.news_sentiment,
            a.published_at,
            current_timestamp as updated_at
		from news_type a 
		left join auxiary_type b on upper(a.company) = upper(b.company)
        where a.news_description is not null)
	select * from news_with_ticket