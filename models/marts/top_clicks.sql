with source as (
    select
        date_trunc('day', original_timestamp) as day_visited,
        split(link, '#') as link,
        context_locale,
        src,
        case
            when link[1] is null then link[0]
            else link[1]
        end as link_to_use
    from {{ ref('upstream', 'int_segment__link_clicked') }}
)

select
    day_visited,
    context_locale,
    src,
    link_to_use as link,
    count(*)
from source
group by 1, 2, 3, 4