create keyspace if not exists meetups with replication = {'class' : 'SimpleStrategy', 'replication_factor':1};

use meetups;

drop table if exists meetups.countries;
drop table if exists meetups.city_groups;
drop table if exists meetups.groups;
drop table if exists meetups.events;
drop table if exists meetups.stats_events;
drop table if exists meetups.stats_groups;
drop table if exists meetups.stats_topics;

create table if not exists meetups.countries
(
    country text,
    city    text,
    primary key ((country), city)
);

create table if not exists meetups.events
(
    event_id        text,
    event_name    text,
    event_time     text,
    group_name    text,
    country text,
    city text,
    topics list<text>,
    primary key (event_id)
);

create table if not exists meetups.city_groups
(
    group_id bigint,
    group_name text,
    city_name text,
    primary key((city_name), group_id)
);

create table if not exists meetups.groups
(
    event_id        text,
    event_name    text,
    event_time     text,
    group_name    text,
    group_id bigint,
    country text,
    city text,
    topics list<text>,

    primary key(group_id)
);

create type if not exists custom_stats_events (country text, number bigint);

create table if not exists meetups.stats_events
(
    time_start text,
    statistics frozen<list<custom_stats_events>>,
    primary key (time_start)
);


create type if not exists custom_stats_groups (state text, events list<text>);

create table if not exists meetups.stats_groups
(
    time_start text,
    statistics frozen<list<custom_stats_groups>>,
    primary key (time_start)
);

create type if not exists custom_stats_topic (topic text, number bigint);

create type if not exists custom_stats_topics (country text, topics list<custom_stats_topic> );

create table if not exists meetups.stats_topics
(
    time_start text,
    statistics      frozen<list<custom_stats_topics>>,
    primary key (time_start)
);
