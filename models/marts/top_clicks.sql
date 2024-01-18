with source as (
    select
        date_trunc('day', original_timestamp) as day_visited,
        split(link, '#') as link,
        context_locale,
        src
    from {{ ref('upstream', 'int_segment__link_clicked') }}
)

select
    day_visited,
    context_locale,
    src,
    link[1] as link,
    count(*) as total_clicks
from source
group by 1, 2, 3, 4