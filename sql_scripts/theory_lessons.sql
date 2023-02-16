-- STAGING
drop table if exists XENIAKUTSEVOL_GMAIL_COM__STAGING.users;
create table XENIAKUTSEVOL_GMAIL_COM__STAGING.users
(
    id              int not null,
    chat_name       varchar(200),
    registration_dt timestamp,
    country         varchar(200),
    age             int
) ORDER BY id
SEGMENTED BY HASH(id) ALL NODES
;

drop table if exists XENIAKUTSEVOL_GMAIL_COM__STAGING.dialogs;
create table XENIAKUTSEVOL_GMAIL_COM__STAGING.dialogs
(
    message_id    int not null,
    message_ts    timestamp,
    message_from  int,
    message_to    int,
    message       varchar(1000),
    message_group int
) ORDER BY message_id
SEGMENTED BY HASH(message_id) ALL NODES
PARTITION BY message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2)
;

drop table if exists XENIAKUTSEVOL_GMAIL_COM__STAGING.groups;
create table XENIAKUTSEVOL_GMAIL_COM__STAGING.groups
(
    id              int not null,
    admin_id        int,
    group_name      varchar(100),
    registration_dt timestamp,
    is_private      boolean
) ORDER BY id, admin_id
SEGMENTED BY HASH(id) ALL NODES
PARTITION BY registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2)
;



-- DWH
-- HUBS
drop table if exists XENIAKUTSEVOL_GMAIL_COM__DWH.h_users;
create table XENIAKUTSEVOL_GMAIL_COM__DWH.h_users
(
    hk_user_id      bigint primary key,
    user_id         int,
    registration_dt datetime,
    load_dt         datetime,
    load_src        varchar(20)
) order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


drop table if exists XENIAKUTSEVOL_GMAIL_COM__DWH.h_groups;
create table XENIAKUTSEVOL_GMAIL_COM__DWH.h_groups
(
    hk_group_id     bigint primary key,
    group_id        int,
    registration_dt datetime,
    load_dt         datetime,
    load_src        varchar(20)
) order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


drop table if exists XENIAKUTSEVOL_GMAIL_COM__DWH.h_dialogs;
create table XENIAKUTSEVOL_GMAIL_COM__DWH.h_dialogs
(
    hk_message_id bigint primary key,
    message_id    int,
    message_ts    datetime,
    load_dt       datetime,
    load_src      varchar(20)
) order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO XENIAKUTSEVOL_GMAIL_COM__DWH.h_users(hk_user_id, user_id, registration_dt, load_dt, load_src)
select hash(id) as hk_user_id,
       id       as user_id,
       registration_dt,
       now()    as load_dt,
       's3'     as load_src
from XENIAKUTSEVOL_GMAIL_COM__STAGING.users
where hash(id) not in (select hk_user_id from XENIAKUTSEVOL_GMAIL_COM__DWH.h_users);

INSERT INTO XENIAKUTSEVOL_GMAIL_COM__DWH.h_groups(hk_group_id, group_id, registration_dt, load_dt, load_src)
select hash(id) as hk_group_id,
       id       as group_id,
       registration_dt,
       now()    as load_dt,
       's3'     as load_src
from XENIAKUTSEVOL_GMAIL_COM__STAGING.groups
where hash(id) not in (select hk_group_id from XENIAKUTSEVOL_GMAIL_COM__DWH.h_groups);

INSERT INTO XENIAKUTSEVOL_GMAIL_COM__DWH.h_dialogs(hk_message_id, message_id, message_ts, load_dt, load_src)
select hash(message_id) as hk_message_id,
       message_id       as message_id,
       message_ts,
       now()            as load_dt,
       's3'             as load_src
from XENIAKUTSEVOL_GMAIL_COM__STAGING.dialogs
where hash(message_id) not in (select hk_message_id from XENIAKUTSEVOL_GMAIL_COM__DWH.h_dialogs);


-- LINKS
drop table if exists XENIAKUTSEVOL_GMAIL_COM__DWH.l_user_message;
create table XENIAKUTSEVOL_GMAIL_COM__DWH.l_user_message
(
    hk_l_user_message bigint primary key,
    hk_user_id        bigint not null
        CONSTRAINT fk_l_user_message_user REFERENCES XENIAKUTSEVOL_GMAIL_COM__DWH.h_users (hk_user_id),
    hk_message_id     bigint not null
        CONSTRAINT fk_l_user_message_message REFERENCES XENIAKUTSEVOL_GMAIL_COM__DWH.h_dialogs (hk_message_id),
    load_dt           datetime,
    load_src          varchar(20)
) order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO XENIAKUTSEVOL_GMAIL_COM__DWH.l_user_message(hk_l_user_message, hk_user_id, hk_message_id, load_dt, load_src)
select hash(hd.hk_message_id, hu.hk_user_id),
       hu.hk_user_id,
       hd.hk_message_id,
       now() as load_dt,
       's3'  as load_src
from XENIAKUTSEVOL_GMAIL_COM__STAGING.dialogs as d
         left join XENIAKUTSEVOL_GMAIL_COM__DWH.h_users as hu on hu.user_id = d.message_from
         left join XENIAKUTSEVOL_GMAIL_COM__DWH.h_dialogs as hd on d.message_id = hd.message_id
where hash(hd.hk_message_id, hu.hk_user_id) not in
      (select hk_l_user_message from XENIAKUTSEVOL_GMAIL_COM__DWH.l_user_message);


drop table if exists XENIAKUTSEVOL_GMAIL_COM__DWH.l_groups_dialogs;
create table XENIAKUTSEVOL_GMAIL_COM__DWH.l_groups_dialogs
(
    hk_l_groups_dialogs bigint primary key,
    hk_message_id       bigint not null
        CONSTRAINT fk_l_groups_dialogs_message REFERENCES XENIAKUTSEVOL_GMAIL_COM__DWH.h_dialogs (hk_message_id),
    hk_group_id         bigint not null
        CONSTRAINT fk_l_groups_dialogs_group REFERENCES XENIAKUTSEVOL_GMAIL_COM__DWH.h_groups (hk_group_id),
    load_dt             datetime,
    load_src            varchar(20)
) order by load_dt
SEGMENTED BY hk_l_groups_dialogs all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

insert into XENIAKUTSEVOL_GMAIL_COM__DWH.l_groups_dialogs(hk_l_groups_dialogs, hk_message_id, hk_group_id, load_dt, load_src)
select hash(hg.hk_group_id, hd.hk_message_id),
       hd.hk_message_id,
       hg.hk_group_id,
       now() as load_dt,
       's3'  as load_src
from XENIAKUTSEVOL_GMAIL_COM__STAGING.dialogs as d
         left join XENIAKUTSEVOL_GMAIL_COM__DWH.h_groups as hg on d.message_group = hg.group_id
         left join XENIAKUTSEVOL_GMAIL_COM__DWH.h_dialogs as hd on d.message_id = hd.message_id
where hg.hk_group_id is not null
  and hash(hg.hk_group_id, hd.hk_message_id) not in
      (select hk_l_groups_dialogs from XENIAKUTSEVOL_GMAIL_COM__DWH.l_groups_dialogs);

drop table if exists XENIAKUTSEVOL_GMAIL_COM__DWH.l_admins;
create table XENIAKUTSEVOL_GMAIL_COM__DWH.l_admins
(
    hk_l_admin_id bigint primary key,
    hk_user_id    bigint not null
        CONSTRAINT fk_l_admins_user REFERENCES XENIAKUTSEVOL_GMAIL_COM__DWH.h_users (hk_user_id),
    hk_group_id   bigint not null
        CONSTRAINT fk_l_admins_group REFERENCES XENIAKUTSEVOL_GMAIL_COM__DWH.h_groups (hk_group_id),
    load_dt       datetime,
    load_src      varchar(20)
) order by load_dt
SEGMENTED BY hk_l_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO XENIAKUTSEVOL_GMAIL_COM__DWH.l_admins(hk_l_admin_id, hk_group_id, hk_user_id, load_dt, load_src)
select hash(hg.hk_group_id, hu.hk_user_id),
       hg.hk_group_id,
       hu.hk_user_id,
       now() as load_dt,
       's3'  as load_src
from XENIAKUTSEVOL_GMAIL_COM__STAGING.groups as g
         left join XENIAKUTSEVOL_GMAIL_COM__DWH.h_users as hu on g.admin_id = hu.user_id
         left join XENIAKUTSEVOL_GMAIL_COM__DWH.h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id, hu.hk_user_id) not in (select hk_l_admin_id from XENIAKUTSEVOL_GMAIL_COM__DWH.l_admins);


-- SATELLITES
drop table if exists XENIAKUTSEVOL_GMAIL_COM__DWH.s_admins;

create table XENIAKUTSEVOL_GMAIL_COM__DWH.s_admins
(
    hk_admin_id bigint not null
        CONSTRAINT fk_s_admins_l_admins REFERENCES XENIAKUTSEVOL_GMAIL_COM__DWH.l_admins (hk_l_admin_id),
    is_admin    boolean,
    admin_from  datetime,
    load_dt     datetime,
    load_src    varchar(20)
) order by load_dt
SEGMENTED BY hk_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO XENIAKUTSEVOL_GMAIL_COM__DWH.s_admins(hk_admin_id, is_admin, admin_from, load_dt, load_src)
select la.hk_l_admin_id,
       True  as is_admin,
       hg.registration_dt,
       now() as load_dt,
       's3'  as load_src
from XENIAKUTSEVOL_GMAIL_COM__DWH.l_admins as la
         left join XENIAKUTSEVOL_GMAIL_COM__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;


drop table if exists XENIAKUTSEVOL_GMAIL_COM__DWH.s_user_socdem;
create table XENIAKUTSEVOL_GMAIL_COM__DWH.s_user_socdem
(
    hk_user_id bigint not null
        CONSTRAINT fk_s_user_socdem_h_users REFERENCES XENIAKUTSEVOL_GMAIL_COM__DWH.h_users (hk_user_id),
    country    varchar(200),
    age        integer,
    load_dt    datetime,
    load_src   varchar(20)
) order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO XENIAKUTSEVOL_GMAIL_COM__DWH.s_user_socdem(hk_user_id, country, age, load_dt, load_src)
select hu.hk_user_id,
       u.country,
       u.age,
       now() as load_dt,
       's3'  as load_src
from XENIAKUTSEVOL_GMAIL_COM__DWH.h_users as hu
         left join XENIAKUTSEVOL_GMAIL_COM__STAGING.users as u on hu.user_id = u.id;


drop table if exists XENIAKUTSEVOL_GMAIL_COM__DWH.s_user_chatinfo;
create table XENIAKUTSEVOL_GMAIL_COM__DWH.s_user_chatinfo
(
    hk_user_id bigint not null
        CONSTRAINT fk_s_user_chatinfo_h_users REFERENCES XENIAKUTSEVOL_GMAIL_COM__DWH.h_users (hk_user_id),
    chat_name  varchar(200),
    load_dt    datetime,
    load_src   varchar(20)
) order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO XENIAKUTSEVOL_GMAIL_COM__DWH.s_user_chatinfo(hk_user_id, chat_name, load_dt, load_src)
select hu.hk_user_id,
       u.chat_name,
       now() as load_dt,
       's3'  as load_src
from XENIAKUTSEVOL_GMAIL_COM__DWH.h_users as hu
         left join XENIAKUTSEVOL_GMAIL_COM__STAGING.users as u on hu.user_id = u.id;


drop table if exists XENIAKUTSEVOL_GMAIL_COM__DWH.s_group_name;
create table XENIAKUTSEVOL_GMAIL_COM__DWH.s_group_name
(
    hk_group_id bigint not null
        CONSTRAINT fk_s_group_name_h_groups REFERENCES XENIAKUTSEVOL_GMAIL_COM__DWH.h_groups (hk_group_id),
    group_name  varchar(100),
    load_dt     datetime,
    load_src    varchar(20)
) order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO XENIAKUTSEVOL_GMAIL_COM__DWH.s_group_name(hk_group_id, group_name, load_dt, load_src)
select hg.hk_group_id,
       g.group_name,
       now() as load_dt,
       's3'  as load_src
from XENIAKUTSEVOL_GMAIL_COM__DWH.h_groups as hg
         left join XENIAKUTSEVOL_GMAIL_COM__STAGING.groups as g on hg.group_id = g.id;

drop table if exists XENIAKUTSEVOL_GMAIL_COM__DWH.s_group_private_status;
create table XENIAKUTSEVOL_GMAIL_COM__DWH.s_group_private_status
(
    hk_group_id bigint not null
        CONSTRAINT fk_s_group_private_status_h_groups REFERENCES XENIAKUTSEVOL_GMAIL_COM__DWH.h_groups (hk_group_id),
    is_private  boolean,
    load_dt     datetime,
    load_src    varchar(20)
) order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO XENIAKUTSEVOL_GMAIL_COM__DWH.s_group_private_status(hk_group_id, is_private, load_dt, load_src)
select hg.hk_group_id,
       g.is_private,
       now() as load_dt,
       's3'  as load_src
from XENIAKUTSEVOL_GMAIL_COM__DWH.h_groups as hg
         left join XENIAKUTSEVOL_GMAIL_COM__STAGING.groups as g on hg.group_id = g.id;

drop table if exists XENIAKUTSEVOL_GMAIL_COM__DWH.s_dialog_info;
create table XENIAKUTSEVOL_GMAIL_COM__DWH.s_dialog_info
(
    hk_message_id bigint not null
        CONSTRAINT fk_s_dialog_info_h_dialogs REFERENCES XENIAKUTSEVOL_GMAIL_COM__DWH.h_dialogs (hk_message_id),
    message       varchar(1000),
    message_from  integer,
    message_to    integer,
    load_dt       datetime,
    load_src      varchar(20)
) order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO XENIAKUTSEVOL_GMAIL_COM__DWH.s_dialog_info(hk_message_id, message, message_from, message_to, load_dt, load_src)
select hd.hk_message_id,
       d.message,
       d.message_from,
       d.message_to,
       now() as load_dt,
       's3'  as load_src
from XENIAKUTSEVOL_GMAIL_COM__DWH.h_dialogs as hd
         left join XENIAKUTSEVOL_GMAIL_COM__STAGING.dialogs as d on hd.message_id = d.message_id;

        
