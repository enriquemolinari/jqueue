CREATE TABLE ar_cpfw_jqueue
( id char(36) NOT NULL,
  channel varchar(100) NOT NULL,
  data text NOT NULL,
  attempt int,
  delay int,
  pushed_at timestamp,
  CONSTRAINT id_pk PRIMARY KEY (id)
);