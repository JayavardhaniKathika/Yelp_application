CREATE TABLE yelp_user
(
yelping_since DATE NOT NULL,
votes varchar2(50) NOT NULL,
review_count number NOT NULL,
name varchar2(50) NOT NULL,
user_id varchar2(100) PRIMARY KEY,
friends clob,
fans long not null,
average_stars float(30) not null,
type varchar2(20) not null,
compliments varchar2(500),
elite varchar2(100)
);

create table yelp_business
(
business_id varchar2(100) primary key,
full_address varchar2(200) not null,
hours varchar2(500),
open varchar2(20),
categories varchar2(500),
city varchar2(100) not null,
review_count number not null,
name varchar2(100) not null,
neighborhoods varchar2(500),
longitude float(30) not null,
state varchar2(10) not null,
stars float(1) not null,
latitude float(30) not null,
attributes varchar2(1800),
type varchar2(20) not null
);


create table yelp_checkin
(
checkin_info varchar2(3000) not null,
type varchar2(50) not null,
business_id varchar2(100) not null,
primary key(checkin_info,business_id),
FOREIGN KEY(business_id) REFERENCES yelp_business(business_id)
);



CREATE TABLE yelp_review 
(
votes varchar2(50) NOT NULL,
user_id varchar2(100) NOT NULL,
review_id varchar2(100) PRIMARY KEY,
stars number NOT NULL,
rdate DATE NOT NULL,
type varchar2(20) NOT NULL,
business_id varchar2(100) NOT NULL,
text clob,
FOREIGN KEY(business_id) REFERENCES yelp_business(business_id),
FOREIGN KEY(user_id) REFERENCES yelp_user(user_id)
);


CREATE TABLE cat
(
business_id varchar2(100) NOT NULL,
category varchar2(500),
sub_category varchar2(500),
FOREIGN KEY(business_id) REFERENCES yelp_business(business_id),
primary key(category,business_id)
);

create table userCount
(
user_id varchar2(100),
nfriends number,
nvotes number
);


create index ind on yelp_business(categories,attributes,stars,business_id);
create index i on cat(category,sub_category);
create index iu on yelp_user(review_count,average_stars,user_id,yelping_since,name);
create index ii on userCount(nfriends,nvotes);
create index ir on yelp_review(user_id,rdate);







