create table if not exists runs (
    id serial primary key,
    run_id varchar not null unique,

    metadata json,
    vendor varchar not null,

    started_at timestamp not null,
    ended_at timestamp
);

create table if not exists datapoints (
    id serial primary key,

    name varchar not null,
    item_id varchar not null,

    quantity decimal not null,
    unit varchar not null,

    price decimal not null,
    discount decimal generated always as (
        case
            when sale_price is null
                then null
            else (price - sale_price) / price * 100
        end
    ) stored,
    sale_price decimal,
    sale_amount decimal generated always as (
        case
            when sale_price is null
                then null
            else price - sale_price
        end
    ) stored,

    unique_key varchar,
    fetched_at timestamp not null,

    run_id int references runs (id) not null,

    unique (run_id, unique_key)
);
-- create trigger if not exists pre_insert_hook before insert on data
