-- staging ddl
drop table if exists XENIAKUTSEVOL_GMAIL_COM__STAGING.group_log;
create table XENIAKUTSEVOL_GMAIL_COM__STAGING.group_log
(
    id identity not null,
    group_id     int,
    user_id      int,
    user_id_from int,
    event        varchar(100),
    datetime     timestamp
)
    ORDER BY id
    SEGMENTED BY HASH(id) ALL NODES
    PARTITION BY datetime::date
        GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);


-- dwh. links
drop table if exists XENIAKUTSEVOL_GMAIL_COM__DWH.l_user_group_activity;
create table XENIAKUTSEVOL_GMAIL_COM__DWH.l_user_group_activity
(
    hk_l_user_group_activity bigint primary key,
    hk_user_id               bigint not null
        CONSTRAINT fk_l_user_group_activity_user REFERENCES XENIAKUTSEVOL_GMAIL_COM__DWH.h_users (hk_user_id),
    hk_group_id              bigint not null
        CONSTRAINT fk_l_user_group_activity_group REFERENCES XENIAKUTSEVOL_GMAIL_COM__DWH.h_groups (hk_group_id),
    load_dt                  datetime,
    load_src                 varchar(20)
)
    order by load_dt
    SEGMENTED BY HASH(hk_l_user_group_activity) all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO XENIAKUTSEVOL_GMAIL_COM__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
select hash(hu.hk_user_id, hg.hk_group_id),
       hu.hk_user_id,
       hg.hk_group_id,
       now() as load_dt,
       's3'  as load_src
from XENIAKUTSEVOL_GMAIL_COM__STAGING.group_log as gl
         left join XENIAKUTSEVOL_GMAIL_COM__DWH.h_users as hu on gl.user_id = hu.user_id
         left join XENIAKUTSEVOL_GMAIL_COM__DWH.h_groups as hg on gl.group_id = hg.group_id
where hash(hu.hk_user_id, hg.hk_group_id) not in
      (select hk_l_user_group_activity from XENIAKUTSEVOL_GMAIL_COM__DWH.l_user_group_activity);


-- dwh. satellites
drop table if exists XENIAKUTSEVOL_GMAIL_COM__DWH.s_auth_history;
create table XENIAKUTSEVOL_GMAIL_COM__DWH.s_auth_history
(
    hk_l_user_group_activity bigint not null
        CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES XENIAKUTSEVOL_GMAIL_COM__DWH.l_user_group_activity (hk_l_user_group_activity),
    user_id_from             int,
    event                    varchar(100),
    event_dt                 timestamp,
    load_dt                  datetime,
    load_src                 varchar(20)
)
    order by load_dt
    SEGMENTED BY HASH(hk_l_user_group_activity) all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO XENIAKUTSEVOL_GMAIL_COM__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt,
                                                        load_dt, load_src)
select distinct luga.hk_l_user_group_activity,
                gl.user_id_from,
                gl.event,
                gl.datetime as event_dt,
                now()       as load_dt,
                's3'        as load_src
from XENIAKUTSEVOL_GMAIL_COM__STAGING.group_log as gl
         left join XENIAKUTSEVOL_GMAIL_COM__DWH.h_groups as hg on gl.group_id = hg.group_id
         left join XENIAKUTSEVOL_GMAIL_COM__DWH.h_users as hu on gl.user_id = hu.user_id
         left join XENIAKUTSEVOL_GMAIL_COM__DWH.l_user_group_activity as luga
                   on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id;


-- analysis
with user_group_messages as (select hk_group_id,
                                    count(distinct hk_user_id) as cnt_users_in_group_with_messages
                             from XENIAKUTSEVOL_GMAIL_COM__DWH.l_user_group_activity luga
                             where hk_user_id in
                                   (select distinct hk_user_id from XENIAKUTSEVOL_GMAIL_COM__DWH.l_user_message)
                             group by hk_group_id)
select hk_group_id,
       cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages;

with user_group_log as (select luga.hk_group_id,
                               count(distinct luga.hk_user_id) as cnt_added_users
                        from XENIAKUTSEVOL_GMAIL_COM__DWH.l_user_group_activity luga
                        where luga.hk_l_user_group_activity in (select hk_l_user_group_activity
                                                                from XENIAKUTSEVOL_GMAIL_COM__DWH.s_auth_history
                                                                where event = 'add')
                          and luga.hk_group_id in (select hk_group_id
                                                   from XENIAKUTSEVOL_GMAIL_COM__DWH.h_groups
                                                   order by registration_dt
                                                   limit 10)
                        group by luga.hk_group_id)
select hk_group_id,
       cnt_added_users
from user_group_log
order by cnt_added_users;

with user_group_messages as (select hk_group_id,
                                    count(distinct hk_user_id) as cnt_users_in_group_with_messages
                             from XENIAKUTSEVOL_GMAIL_COM__DWH.l_user_group_activity luga
                             where hk_user_id in
                                   (select distinct hk_user_id from XENIAKUTSEVOL_GMAIL_COM__DWH.l_user_message)
                             group by hk_group_id),
     user_group_log as (select luga.hk_group_id,
                               count(distinct luga.hk_user_id) as cnt_added_users
                        from XENIAKUTSEVOL_GMAIL_COM__DWH.l_user_group_activity luga
                        where luga.hk_l_user_group_activity in (select hk_l_user_group_activity
                                                                from XENIAKUTSEVOL_GMAIL_COM__DWH.s_auth_history
                                                                where event = 'add')
                          and luga.hk_group_id in (select hk_group_id
                                                   from XENIAKUTSEVOL_GMAIL_COM__DWH.h_groups
                                                   order by registration_dt
                                                   limit 10)
                        group by luga.hk_group_id)
SELECT ugl.hk_group_id,
       ugl.cnt_added_users,
       ugm.cnt_users_in_group_with_messages,
       round(ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users * 100, 1) as group_conversion
from user_group_log ugl
         left join user_group_messages ugm
                   on ugl.hk_group_id = ugm.hk_group_id
order by group_conversion desc;

