with source as (
    select
        date_trunc('day', original_timestamp) as day_visited,
        split(context_page_title, ' - ')[0]::string as page,
        context_locale,
        src,
        case
            when page = src then 'home'
            else page
        end as actual_page
    from {{ ref('upstream', 'int_segment__link_clicked') }}
)

select
    day_visited,
    context_locale,
    src,
    actual_page,
    count(*) as total_visits
from source
group by 1, 2, 3, 4