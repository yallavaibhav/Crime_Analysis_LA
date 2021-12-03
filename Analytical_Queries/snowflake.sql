#The count of  status of crime in the year 2021 in descending order.  
select Status_desc,count(STATUS_DESC) as Number_of_Crime_statuses
from crime_data_TF C
inner join Date_information d
on d.ID = C.ID
where Year_occ = 2021
group by C.Status_desc
order by count(STATUS_DESC) desc;

#The total number of crimes that happened in a specific area, which victim gender was targeted and what kind of crime occurred in a CUBE format.
select area_name,vict_sex,CRM_CD_DESC,count(crm_id) as Number_of_Crimes
from crime_data_TF f
Group by CUBE(AREA_NAME,vict_sex,CRM_CD_DESC)
order by count(crm_id) desc;

-- Number of crime for every year, Quarter and Month from 2010 to 2021 
select D.year_occ,D.Quarter_NAME_OCC,D.Month_NAME_OCC,count(f.crm_id) as Numer_of_crimes from crime_data_TF f
inner join Date_information D
on f.id = D.id 
Group by Rollup(D.year_occ,D.Quarter_NAME_OCC,D.Month_NAME_OCC)
order by count(f.crm_id) desc;

-- Ranking the victims who were targeted for a specific crime based on Victim’s age .
Select f.area_id,f.AREA_NAME,f.crm_cd_desc,f.vict_age,
RANK() over (ORDER BY f.vict_age DESC) as Rank_Victim_Age_area
from crime_data_TF f;

-- Victims who were targeted for a specific crime based on victim’s age in a particular area
select Area_name,count(distinct DR_no) as Number_of_crimes from Crime_DATA_TF
group by Area_name
order by Number_of_crimes desc;

-- Number of Crimes according to Area
select Area_name,count(distinct DR_no) as Number_of_crimes from Crime_DATA_TF
group by Area_name
order by Number_of_crimes desc;

--  In the year 2021, the total number of crimes occurred for specific race, specific area and {specific area, specific race}
select C.area_name,VD.VICT_DESCENT_DESC,count(C.DR_NO)
from crime_data_TF C
inner join VICTIM_descent VD
on C.VICT_DESCENT = VD.VICT_DESCENT
inner join Date_information D
on C.id = D.id
where YEAR_OCC =2021
Group by CUBE(AREA_NAME,VD.VICT_DESCENT_DESC)
order by count(C.DR_NO) desc;   

-- Most used weapons used against victim in the year 2021 
select Weapon_DESC, count(WEAPON_USED_ID) from Crime_DATA_TF C
inner join Date_information D
on C.id = D.id
where year_occ = 2021 and WEAPON_DESC!='NULL'
group by Weapon_DESC
order by count(WEAPON_USED_ID) desc;

-- Top 5 crime prone area in the year 2021
select area_name,count(DR_NO) from Crime_DATA_TF C
inner join Date_information D
on C.id = D.id
where year_occ = 2021
group by area_name
order by count(DR_NO) desc
limit 5;

-- Age window suseptible to be more victimized
select C.VICT_AGE, count(C.VICT_AGE) from Crime_DATA_TF C
inner join Date_information D
on C.id = D.id
where year_occ = 2021 and VICT_AGE!=0
group by VICT_AGE
order by count(VICT_AGE) desc;

-- Victim descent that is more susiptible in the year 2021
select V.VICT_DESCENT_DESC, count(C.VICT_DESCENT) from Crime_DATA_TF C
inner join Date_information D
on C.id = D.id
inner join VICTIM_DESCENT V
on V.VICT_DESCENT = C.VICT_DESCENT
where year_occ = 2021
group by VICT_DESCENT_DESC
order by count(C.VICT_DESCENT) desc;

-- Which premise has been more affected in crime in the year 2021
select PREMISE_DESC,count(DR_NO) from Crime_DATA_TF C
inner join Date_information D
on C.id = D.id
where year_occ = 2021
group by PREMISE_DESC
order by count(DR_NO) desc;
