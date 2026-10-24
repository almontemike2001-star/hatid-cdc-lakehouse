-- Hatid operational database (synthetic). The `app` schema is what the delivery app writes;
-- Debezium captures every insert, update and delete on it.
create schema app;
create schema sim;  -- simulator bookkeeping, deliberately not captured

create table app.customers (
    customer_id  bigint primary key,
    full_name    text not null,
    email        text not null,
    city         text not null,
    created_at   timestamptz not null,
    updated_at   timestamptz not null
);

create table app.restaurants (
    restaurant_id bigint primary key,
    name          text not null,
    cuisine       text not null,
    city          text not null,
    rating        numeric(2, 1) not null,
    is_active     boolean not null default true,
    created_at    timestamptz not null,
    updated_at    timestamptz not null
);

create table app.riders (
    rider_id    bigint primary key,
    full_name   text not null,
    city        text not null,
    vehicle     text not null,
    is_active   boolean not null default true,
    created_at  timestamptz not null,
    updated_at  timestamptz not null
);

create table app.orders (
    order_id       bigint primary key,
    -- account deletion anonymises past orders instead of deleting them
    customer_id    bigint references app.customers (customer_id) on delete set null,
    restaurant_id  bigint not null references app.restaurants (restaurant_id),
    status         text not null,
    subtotal       numeric(10, 2) not null,
    delivery_fee   numeric(10, 2) not null,
    placed_at      timestamptz not null,
    accepted_at    timestamptz,
    ready_at       timestamptz,
    cancelled_at   timestamptz,
    cancel_reason  text,
    updated_at     timestamptz not null
);

create table app.deliveries (
    delivery_id   bigint primary key,
    order_id      bigint not null unique references app.orders (order_id),
    rider_id      bigint not null references app.riders (rider_id),
    status        text not null,
    distance_km   numeric(5, 2) not null,
    promised_at   timestamptz not null,
    assigned_at   timestamptz not null,
    picked_up_at  timestamptz,
    delivered_at  timestamptz,
    updated_at    timestamptz not null
);

-- Full before-images, so a DELETE event still carries every column.
alter table app.customers replica identity full;
alter table app.restaurants replica identity full;
alter table app.riders replica identity full;
alter table app.orders replica identity full;
alter table app.deliveries replica identity full;

create table sim.state (
    id               int primary key default 1,
    sim_clock        timestamptz not null,
    next_order_id    bigint not null,
    next_customer_id bigint not null,
    seed             bigint not null
);

-- Lifecycle events scheduled after the end of the last simulated window.
create table sim.pending (
    event_time  timestamptz not null,
    seq         bigint not null,
    kind        text not null,
    payload     jsonb not null
);
