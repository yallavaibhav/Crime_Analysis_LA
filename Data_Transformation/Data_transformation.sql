alter table crime_data drop column RPT_DIST_NO,"Part 1-2",MOCODES,CRM_CD1,CRM_CD2,CRM_CD3,CRM_CD4; --removing columns that are not needed for analysis and have null values
update  crime_data
 set WEAPON_USED_CD='500' where WEAPON_USED_CD is null ; --as code 500 is for unknown so replacing all the null with 500

CREATE OR REPLACE  TABLE crime_data_transformed AS   --creating a new transformed table from the raw table with necessary transformations
SELECT
     DR_NO::BIGINT AS DR_NO,
     to_date(Date_Rptd,'MM/dd/yyyy HH12:MI:SS AM') AS date_rptd,  --getting just the date part as the time was same for all rows
     to_date(DATE_OCC,'MM/dd/yyyy HH12:MI:SS AM') AS date_occ,
     time_from_parts(floor(time_occ::int / 100) % 24,time_occ::int % 100,0,0) as time_occ, --converting military time to time
     "AREA "::integer as area_id,           --changing data types
     AREA_NAME::string as area_name,
     crmcd::integer as crm_id,
     CRM_CD_DESC::string as crm_cd_desc,
     "Vict _Age"::integer as vict_age,
     VICT_SEX::string as vict_sex,
     VICT_DESCENT::string as vict_descent,
     PREMIS_CD::integer as premise_id,
     PREMIS_DESC::string as premise_desc,
     WEAPON_USED_CD::int as weapon_used_id,
     WEAPONDESC::string as weapon_desc,
     "STATUS"::string as crime_status,
     STATUS_DESC as status_desc,
     LOCATION,
     CROSS_STREET,
     LAT::float as latitude,
     LON::float as longitude
     FROM crime_data;


 select distinct(vict_sex)from crime_data_transformed;
  update crime_data_transformed
  set vict_sex='X'
  WHERE vict_sex is null or vict_sex='H' or vict_sex='-' or vict_sex='N';

  select distinct(VICT_DESCENT)from crime_data_transformed;
  update crime_data_transformed
  set VICT_DESCENT='X'
  where VICT_DESCENT='-' or VICT_DESCENT is null;

   select DISTINCT(crime_status) from crime_data_transformed
   where status_desc ='UNK'
   select   distinct(status_desc) from crime_data_transformed;

    update crime_data_transformed
   set CRIME_STATUS='TH'
   where crime_status='19' or CRIME_STATUS='CC' OR CRIME_STATUS IS null or CRIME_STATUS='13';
