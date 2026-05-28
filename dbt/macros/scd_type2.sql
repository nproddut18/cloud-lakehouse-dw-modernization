{#
  scd_type2 macro — generates a full SCD Type 2 dimension table.

  Parameters:
    source_ref     : ref() to the source model
    unique_key     : natural key column name (e.g. 'customer_id')
    surrogate_key  : name for the generated surrogate key column
    tracked_cols   : list of columns to track for changes
    effective_from : name for the start date column
    effective_to   : name for the end date column (null = current)
    current_flag   : name for the is_current boolean flag
#}

{% macro scd_type2(source_ref, unique_key, surrogate_key, tracked_cols,
                   effective_from='effective_start_date',
                   effective_to='effective_end_date',
                   current_flag='is_current') %}

with source as (
    select * from {{ source_ref }}
),

{# Build a hash of tracked columns to detect changes #}
hashed as (
    select
        *,
        {{ dbt_utils.surrogate_key(tracked_cols) }} as _row_hash

    from source
),

{# Retrieve existing dimension rows #}
{% if is_incremental() %}
existing as (
    select * from {{ this }}
),

{# Find records that have changed #}
changed as (
    select
        s.{{ unique_key }},
        s._row_hash as new_hash,
        e._row_hash as old_hash

    from hashed s
    inner join existing e
        on s.{{ unique_key }} = e.{{ unique_key }}
        and e.{{ current_flag }} = true
    where s._row_hash != e._row_hash
),

{# Expire old rows for changed records #}
expired as (
    select
        e.*,
        current_date                                as {{ effective_to }},
        false                                       as {{ current_flag }}

    from existing e
    inner join changed c using ({{ unique_key }})
    where e.{{ current_flag }} = true
),

{# New rows: net-new keys + changed keys #}
new_rows as (
    select
        {{ dbt_utils.surrogate_key([unique_key, "'current'"])}} as {{ surrogate_key }},
        s.*,
        current_date                                as {{ effective_from }},
        cast(null as date)                          as {{ effective_to }},
        true                                        as {{ current_flag }}

    from hashed s
    where s.{{ unique_key }} not in (select {{ unique_key }} from existing where {{ current_flag }} = true)
       or s.{{ unique_key }} in (select {{ unique_key }} from changed)
),

final as (
    select * from existing
    where {{ unique_key }} not in (select {{ unique_key }} from expired)

    union all select * from expired
    union all select * from new_rows
)

{% else %}

{# Initial full load #}
final as (
    select
        {{ dbt_utils.surrogate_key([unique_key, "'current'"])}} as {{ surrogate_key }},
        *,
        current_date                                as {{ effective_from }},
        cast(null as date)                          as {{ effective_to }},
        true                                        as {{ current_flag }}

    from hashed
)

{% endif %}

select * from final

{% endmacro %}
