 CREATE OR REPLACE STORAGE INTEGRATION tableau_Integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::825765422200:role/tableau.role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://tableau.project/')
  COMMENT = 'Optional Comment'


  //description Integration Object
  desc integration tableau_Integration;

//drop integration PBI_Integration

CREATE database tableau;

create schema tableau_Data;

create table tableau_dataset (
Household_ID	string,Region	string,Country	string,Energy_Source	string,
Monthly_Usage_kWh	float,Year	int,Household_Size	int,Income_Level	string,
Urban_Rural	string,Adoption_Year	int,Subsidy_Received	string,Cost_Savings_USD float




);


select * from tableau_dataset;

//drop database tableau;

create stage tableau.tableau_Data.tableau_stage
url = 's3://renewable.tableau.proj'
storage_integration = tableau_Integration

//desc stage s1

//drop stage s1;


copy into tableau_dataset 
from @tableau_stage
file_format = (type=csv field_delimiter=',' skip_header=1 )
on_error = 'continue'

list @s1

select region,count(*) from tableau_dataset group by region;

create table energy_consumption as
select * from tableau_dataset;

select * from energy_consumption;

select income_level,count(*) from energy_consumption group by income_level;

update energy_consumption
set monthly_usage_kwh = monthly_usage_kwh*1.1
where income_level ='Low'

update energy_consumption
set monthly_usage_kwh = monthly_usage_kwh*1.2
where income_level ='Middle'

update energy_consumption
set monthly_usage_kwh = monthly_usage_kwh*1.3
where income_level ='High'

update energy_consumption
set cost_savings_usd = cost_savings_usd*0.9
where income_level ='Low'

update energy_consumption
set cost_savings_usd = cost_savings_usd*0.8
where income_level ='Middle'

update energy_consumption
set cost_savings_usd = cost_savings_usd*0.7
where income_level ='High'