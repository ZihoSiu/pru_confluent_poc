/* Docker console access to ksqlDB node */
docker exec -it ksqldb-cli bash ksql http://ksqldb-server:8088
/* ksqlDB Set offset to 0 */
set 'auto.offset.reset'='earliest';

/* Create Source Stream cdb_mast */
CREATE STREAM ST_cdb_mast(
  client_cd VARCHAR KEY,
  type VARCHAR,
  cdb_mast_sta VARCHAR,
  role VARCHAR,
  surname VARCHAR,
  first_name VARCHAR,
  other_name VARCHAR,
  id_no VARCHAR,
  sex VARCHAR,
  dob INT,
  birth_place VARCHAR,
  smk_sta VARCHAR,
  occ_cd VARCHAR,
  ann_salary INT,
  height INT,
  height_uom VARCHAR,
  weight INT,
  weight_uom VARCHAR,
  ntu VARCHAR,
  supp_ind VARCHAR,
  sl_no VARCHAR,
  entry_dt VARCHAR,
  entry_by VARCHAR,
  opt_out VARCHAR,
  podium_iud VARCHAR
)
  WITH (KAFKA_TOPIC='cdb_mast', FORMAT='JSON_SR');

/* Create Source Stream cdb_misc */
CREATE STREAM ST_cdb_misc (
  client_cd VARCHAR KEY,
  misc_type VARCHAR,
  misc_cd VARCHAR,
  last_upd_dt BIGINT,
  last_upd_usr BIGINT,
  podium_iud VARCHAR
  )
  WITH (KAFKA_TOPIC='cdb_misc', FORMAT='JSON_SR');

/*
CREATE TABLE TB_cdb_misc AS
  SELECT
    count(1),
    client_cd,
    misc_type,
    misc_cd,
    last_upd_dt,
    last_upd_usr,
    podium_iud
  FROM ST_cdb_misc
  GROUP BY client_cd, misc_type, misc_cd, last_upd_dt, last_upd_usr, podium_iud
  EMIT CHANGES;
*/

/* Create Source Stream pol_person_hdr */
CREATE STREAM ST_pol_person_hdr(
  pol_no VARCHAR,
  client_role VARCHAR,
  client_cd VARCHAR KEY,
  efd VARCHAR,
  relation VARCHAR,
  split VARCHAR,
  category VARCHAR,
  priority VARCHAR,
  production_type VARCHAR,
  addr_no VARCHAR,
  podium_iud VARCHAR
)
  WITH (KAFKA_TOPIC='pol_person_hdr', FORMAT='JSON_SR');


CREATE STREAM ST_client_pol_master_test2
  AS SELECT mast.client_cd,
    type,
    role,
    surname,
    first_name,
    other_name,
    id_no,
    sex,
    dob,
    opt_out,
    pph.efd,
    misc.last_upd_dt,
    mast.podium_iud,
    pph.podium_iud,
    misc.podium_iud
    FROM ST_cdb_mast mast
  LEFT JOIN ST_pol_person_hdr pph WITHIN 7 DAYS on mast.client_cd = pph.client_cd
  LEFT JOIN ST_cdb_misc misc WITHIN 7 DAYS on mast.client_cd = misc.client_cd;

CREATE STREAM target_sex_joined_table
  AS SELECT mast.client_cd, surname, first_name, other_name, pol_no, sex, efd, last_upd_dt
    FROM cdb_mast mast
  LEFT JOIN pol_person_hdr pph on mast.client_cd = pph.client_cd
  LEFT JOIN cdb_misc misc on mast.client_cd = misc.client_cd
  WHERE sex='M';


/* Ziho queries */

CREATE STREAM cdb_mast (client_cd_k VARCHAR KEY)
    WITH (KAFKA_TOPIC='cdb_mast', FORMAT='JSON_SR');

CREATE STREAM pol_person_hdr (pol_no_k VARCHAR KEY)
      WITH (KAFKA_TOPIC='pol_person_hdr', FORMAT='JSON_SR');

CREATE STREAM cdb_misc (misc_cd_k VARCHAR KEY)
      WITH (KAFKA_TOPIC='cdb_misc', FORMAT='JSON_SR');

docker exec -it kafka kafka-console-producer --bootstrap-server kafka:9092 --topic gender -property schema.registry.url=http://schema-registry:8081 --property parse.key=true --property key.separator=,
M,{"sex_code":"M","long_form":"Male"}
F,{"sex_code":"F","long_form":"Female"}
NA,{"sex_code":"NA","long_form":"N/A"}

CREATE STREAM gender (sex_code VARCHAR KEY, long_form VARCHAR)
    WITH (KAFKA_TOPIC='gender', VALUE_FORMAT='JSON');

CREATE TABLE gender_table as
  SELECT sex_code, latest_by_offset(long_form) long_form
  FROM gender
  GROUP BY sex_code;
/*
CREATE STREAM enriched_cdb_mast as
  SELECT m.client_cd, m.sex, g.long_form sex_long_form
  FROM cdb_mast m
  LEFT JOIN gender_table g
  ON m.sex = g.sex_code
  PARTITION BY m.client_cd;
*/

-- Case 1.1

CREATE STREAM STM_CASE_1_1 as
  SELECT m.client_cd_k client_cd, /*m.type, m.role, */m.surname, m.first_name, m.other_name, /*m.id_no, g.long_form sex, m.dob, p.efd, */m.opt_out, m.entry_dt
  FROM cdb_mast m
  /*LEFT JOIN gender_table g
  ON m.sex = g.sex_code
  LEFT JOIN pol_person_hdr p WITHIN 7 DAYS
  ON m.client_cd = p.client_cd*/
  WHERE m.podium_iud = 'I' or m.podium_iud = 'U';

CREATE TABLE TBL_CASE_1_1 as
  SELECT client_cd, /*latest_by_offset(m.type) type, latest_by_offset(m.role) role, */latest_by_offset(surname) surname,
  latest_by_offset(first_name) first_name, latest_by_offset(other_name) other_name, /*latest_by_offset(m.id_no) id_no,
  latest_by_offset(m.sex) sex, latest_by_offset(m.dob) dob, latest_by_offset(m.efd) efd, */latest_by_offset(opt_out) opt_out,
  latest_by_offset(entry_dt) Last_Update_dt
  FROM STM_CASE_1_1
  GROUP BY client_cd;

-- Case 1.2

CREATE STREAM STM_CASE_1_2 as
  SELECT m.client_cd_k client_cd, /*m.type, m.cdb_mast_sta, m.birth_place, p.efd, m.sex, */m.opt_out, misc.last_upd_dt
  FROM cdb_misc misc
  LEFT JOIN cdb_mast m WITHIN 7 DAYS
  ON m.client_cd_k = misc.client_cd
  /*LEFT JOIN pol_person_hdr p WITHIN 7 DAYS
  ON m.client_cd = p.client_cd*/
  WHERE misc.podium_iud = 'I' or misc.podium_iud = 'U';

CREATE TABLE TBL_CASE_1_2 as
  SELECT client_cd, /*latest_by_offset(type) type, latest_by_offset(cdb_mast_sta) cdb_mast_sta,
  latest_by_offset(birth_place) birth_place, latest_by_offset(efd) efd, */latest_by_offset(opt_out) opt_out,
  /*latest_by_offset(sex) gender, */latest_by_offset(last_upd_dt) Last_Update_dt
    FROM STM_CASE_1_2
    GROUP BY client_cd;

-- Case 2.1

CREATE STREAM STM_CASE_2_1 as
  SELECT p.client_cd client_cd, /*m.type, m.role, m.surname, m.first_name, m.other_name, m.sex, m.id_no, m.dob, */p.efd, /*m.opt_out, */m.entry_dt
  FROM pol_person_hdr p
  LEFT JOIN cdb_mast m WITHIN 7 DAYS
  ON m.client_cd_k = p.client_cd
  WHERE p.podium_iud = 'I' or p.podium_iud = 'U';
  --PARTITION BY p.client_cd;

CREATE TABLE TBL_CASE_2_1 as
  SELECT client_cd, /*latest_by_offset(m.type) type, latest_by_offset(m.role) role, latest_by_offset(m.surname) surname,
  latest_by_offset(m.first_name) first_name, latest_by_offset(m.other_name) other_name, latest_by_offset(m.id_no) id_no,
  latest_by_offset(m.sex) sex, latest_by_offset(m.dob) dob, */latest_by_offset(efd) efd, /*latest_by_offset(m.opt_out) opt_out,
  */latest_by_offset(entry_dt) Last_Update_dt
  FROM STM_CASE_2_1
  GROUP BY client_cd;

-- Case 2.2

CREATE STREAM STM_CASE_2_2 as
  SELECT p.client_cd client_cd, /*m.type, m.cdb_mast_sta, m.birth_place, */p.efd, /*m.sex, m.opt_out, */m.entry_dt
  FROM pol_person_hdr p
  LEFT JOIN cdb_mast m WITHIN 7 DAYS
  ON m.client_cd_k = p.client_cd
  WHERE p.podium_iud = 'I' or p.podium_iud = 'U';

CREATE TABLE TBL_CASE_2_2 as
  SELECT client_cd, /*latest_by_offset(type) type, latest_by_offset(cdb_mast_sta) cdb_mast_sta,
  latest_by_offset(birth_place) birth_place, */latest_by_offset(efd) efd, /*latest_by_offset(opt_out) opt_out,
  latest_by_offset(sex) gender, */latest_by_offset(entry_dt) Last_Update_dt
    FROM STM_CASE_2_2
    GROUP BY client_cd;

-- Case 3

CREATE STREAM STM_CASE_3 as
  SELECT m.client_cd client_cd, /*m.type, m.cdb_mast_sta, m.birth_place, p.efd, */g.long_form gender, /*m.opt_out, */m.entry_dt
  FROM cdb_mast m
  LEFT JOIN gender_table g
  ON m.sex = g.sex_code
  /*LEFT JOIN pol_person_hdr p WITHIN 7 DAYS
  ON m.client_cd = p.client_cd*/
  WHERE m.podium_iud = 'I' or m.podium_iud = 'U'
  PARTITION BY m.client_cd;

CREATE TABLE TBL_CASE_3 as
  SELECT client_cd, /*latest_by_offset(type) type, latest_by_offset(cdb_mast_sta) cdb_mast_sta,
  latest_by_offset(birth_place) birth_place, latest_by_offset(efd) efd, latest_by_offset(opt_out) opt_out,
  */latest_by_offset(gender) gender, latest_by_offset(entry_dt) Last_Update_dt
    FROM STM_CASE_3
    GROUP BY client_cd;

-- Case 4.1

CREATE STREAM TARGET_SEX_JOINED_1 as
  SELECT m.client_cd client_cd, m.surname, m.first_name, m.other_name, p.pol_no, p.efd, m.sex, m.entry_dt
  FROM cdb_mast m
  LEFT JOIN pol_person_hdr p WITHIN 7 DAYS
  ON m.client_cd = p.client_cd
  WHERE m.podium_iud = 'U' and m.sex = 'M'
  PARTITION BY p.pol_no;

CREATE TABLE target_sex_joined_table_1 as
  SELECT pol_no, latest_by_offset(client_cd), latest_by_offset(surname) surname, latest_by_offset(first_name) first_name,
  latest_by_offset(other_name) other_name, latest_by_offset(sex) sex,
  latest_by_offset(entry_dt) entry_dt
    FROM target_sex_joined_1
    GROUP BY pol_no;

-- Case 4.2

CREATE STREAM TARGET_SEX_JOINED_2 as
  SELECT p.client_cd client_cd, m.surname, m.first_name, m.other_name, p.pol_no, p.efd, m.sex, m.entry_dt
  FROM pol_person_hdr p
  LEFT JOIN cdb_mast m WITHIN 7 DAYS
  ON p.client_cd = m.client_cd
  WHERE p.podium_iud = 'U' and m.sex = 'M';

CREATE TABLE target_sex_joined_table_2 as
  SELECT client_cd, latest_by_offset(surname) surname, latest_by_offset(first_name) first_name,
  latest_by_offset(other_name) other_name, latest_by_offset(pol_no) pol_no, latest_by_offset(sex) sex,
  latest_by_offset(entry_dt) entry_dt
    FROM target_sex_joined_2
    GROUP BY client_cd;


-- Case 4.3

CREATE STREAM TARGET_SEX_JOINED_3 as
  SELECT p.client_cd client_cd, m.surname, m.first_name, m.other_name, p.pol_no, p.efd, m.sex, m.entry_dt
  FROM pol_person_hdr p
  LEFT JOIN cdb_mast m WITHIN 7 DAYS
  ON p.client_cd = m.client_cd
  WHERE p.podium_iud = 'I' and m.sex = 'M';

CREATE TABLE target_sex_joined_table_3 as
  SELECT client_cd, latest_by_offset(surname) surname, latest_by_offset(first_name) first_name,
  latest_by_offset(other_name) other_name, latest_by_offset(pol_no) pol_no, latest_by_offset(sex) sex,
  latest_by_offset(entry_dt) entry_dt
    FROM target_sex_joined_3
    GROUP BY client_cd;
