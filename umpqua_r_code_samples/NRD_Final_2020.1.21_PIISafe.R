#Package Dependancies##########################################################################################
library(readxl)
library(dplyr)
library(stringr)
library(openxlsx)
library(getPass)
library(odbc)
library(DBI)
library(RODBC)
library(lubridate)
library(tidyr)

print(paste0("Started at: ", Sys.time()))
options(scipen=999) #gets rid of scientific notation

#Database Connections##########################################################################################
dbicon <- DBI::dbConnect(odbc::odbc(),
                         
                         driver = "SQL Server",
                         
                         server =  "",
                         
                         database = "EDW_Sandbox"
                         
)

dbitran <- DBI::dbConnect(odbc::odbc(),
                          
                          driver = "SQL Server",
                          
                          
                          server =  "",
                          
                          database = "AccountTransactions"
)
#Manual Fields##########################################################################################

#change to drive letter assigned to the 938 share drive
drive = "Y"

#Date Variable Generation##########################################################################################

#Retrieve past 4 full dates 
# Simple data.frame trick to always get the most recent full date
DF <- data.frame(DATE = seq(as.Date(floor_date(Sys.Date(), "month") - days(150)), as.Date(floor_date(Sys.Date(), "month") - days(1)), "day"))

dates = DF %>%
  mutate(month = months(DATE), weekday = weekdays(DATE)) %>%
  group_by(month) %>%
  filter(!weekday %in% c("Saturday", "Sunday")) %>%
  summarise(last_weekday = max(DATE))

dates = dates %>% arrange(last_weekday)

#Set Full Date and Floor Date Variables 
#Convert to strings, this does not need to be changed

#M1
M1_full_date = dates$last_weekday[5]
M1_full_date_string=toString(sprintf("'%s'", M1_full_date)) 
M1_first_day = floor_date(ymd(M1_full_date), "month")
M1_first_day_string=toString(sprintf("'%s'", M1_first_day)) 
M1_last_day = ceiling_date(ymd(M1_full_date), "month") - 1
M1_last_day_string=toString(sprintf("'%s'", M1_last_day))
#M2
M2_full_date = dates$last_weekday[4]
M2_full_date_string=toString(sprintf("'%s'", M2_full_date)) 
M2_first_day = floor_date(ymd(M2_full_date), "month")
M2_first_day_string=toString(sprintf("'%s'", M2_first_day)) 
M2_last_day = ceiling_date(ymd(M2_full_date), "month") - 1
M2_last_day_string=toString(sprintf("'%s'", M2_last_day)) 
#M3
M3_full_date = dates$last_weekday[3]
M3_full_date_string=toString(sprintf("'%s'", M3_full_date)) 
M3_first_day = floor_date(ymd(M3_full_date), "month")
M3_first_day_string=toString(sprintf("'%s'", M3_first_day)) 
M3_last_day = ceiling_date(ymd(M3_full_date), "month") - 1
M3_last_day_string=toString(sprintf("'%s'", M3_last_day))
#M4 
M4_full_date = dates$last_weekday[2]
M4_full_date_string=toString(sprintf("'%s'", M4_full_date)) 
M4_first_day = floor_date(ymd(M4_full_date), "month")
M4_first_day_string=toString(sprintf("'%s'", M4_first_day)) 
M4_last_day = ceiling_date(ymd(M4_full_date), "month") - 1
M4_last_day_string=toString(sprintf("'%s'", M4_last_day))
#M5
M5_full_date = dates$last_weekday[1]
M5_full_date_string=toString(sprintf("'%s'", M5_full_date)) 
M5_first_day = floor_date(ymd(M5_full_date), "month")
M5_first_day_string=toString(sprintf("'%s'", M5_first_day)) 
M5_last_day = ceiling_date(ymd(M5_full_date), "month") - 1
M5_last_day_string=toString(sprintf("'%s'", M5_last_day))

#MONTH 3 Population Creation##########################################################################################

#Start with trimmed down number of services 

numb_svcs = "
SELECT distinct
ns.[HH Durable Key]
--,ns.[Line of Business]
--,ns.[Market]
--,ns.[Region]
--,ns.[Area]
--,ns.[Store Number]
--,ns.[Store Name]
,ns.[Full Date]
,ns.[New HH This Month]
--,ns.[H Secondary Officer Number]
,ns.[Primary Customer Number]
FROM [EDW_Sandbox].[dbo].[Number_Of_Services_HST] ns
Where ns.[Full Date] = %s
and ns.[Line of Business] = 'Retail Banking'
and ns.[DDA] = 1
and ns. [Household Type] in ('Consumer', 'Mixed')
"
#Apply fulldate string to SQL statement
numb_svcs_M3_string = sprintf(numb_svcs, M3_full_date_string)
#Retrieve data using SQL statement and full date
M3_starting_df = DBI::dbGetQuery(dbicon, numb_svcs_M3_string)

#Idetinfy qualifying Accounts

qual_dda_sql = "
SELECT DISTINCT
a.[AccountNumber]
,cr.[EDW Customer ID] 
,a.AccountOpenDate
,a.[ClosedDate]
,cr.[HH Durable Key]
,cr.[Primary Customer Code]
,cc.[CostCenter] as 'Store Number'
,a.[SecondaryOfficerCode] as 'Associate ID'
,AVG(d.[AverageLedgerBalanceMTD]) as 'AverageLedgerBalanceMTD'
FROM [EDWLoansandDeposits].[dbo].[Accounts] a
inner join [EDWUBCentralData].[dbo].[vw_GL_Hierarchy] cc
on cc.[CostCenter_ID] = a.[ServicingCostCenter_ID]
inner join [EDWLoansandDeposits].[dbo].[DepositsMonthly] d
on a.Account_ID = d.Account_ID
inner join [EDWLoansandDeposits].[dbo].[Products] p
on a.Product_ID = p.Product_ID
and p.Product_ID in ('178', '208', '250', '283')
inner Join [EDW_Sandbox].[dbo].[Customer_Analytics_XRef_TEST]cr
ON RIGHT('00000000000' + CAST(cr.[Account Number] AS Varchar),11) = CAST(a.AccountNumber AS varchar)
and cr.[Primary Customer Code] = 'P'
and cr.[Account Relationship Type Desc] in ('INDIVIDUAL', 'JOINT (OR)')
and cr.[Acct Prod Product Desc] != 'ONLINE STATEMENTS'
AND cr.[Full Date] = %s
inner Join [EDWLoansandDeposits].[dbo].[Customers] c
ON c.Customer_ID = cr.[EDW Customer ID]
WHERE (a.AccountOpenDate BETWEEN %s AND %s) -- account was opened in target month
AND (a.[ClosedDate] > %s OR a.ClosedDate is null)
and a.[Datasource_ID] = 1
and d.DateKey BETWEEN %s AND %s
group by a.[AccountNumber]
,cr.[EDW Customer ID] 
,a.AccountOpenDate
,a.[ClosedDate]
,cr.[HH Durable Key]
,cr.[Primary Customer Code]
,cc.[CostCenter]
,a.[SecondaryOfficerCode]
"
#Apply fulldate string to SQL statement
qual_dda_sql_string = sprintf(qual_dda_sql, M3_full_date_string, M3_first_day_string, M3_last_day_string, M3_last_day_string, M3_first_day_string, M1_last_day_string)

#Starter pop is HHs that opened a qualifying checking account in M3
qual_dda = DBI::dbGetQuery(dbicon, qual_dda_sql_string)
qual_dda$AccountClosed = ifelse((is.na(qual_dda$ClosedDate)| qual_dda$ClosedDate > M1_last_day), "Open", "Closed")
qual_dda$DaysOpen = ifelse(qual_dda$AccountClosed == "Closed", round(difftime(qual_dda$ClosedDate , qual_dda$AccountOpenDate, units = c("days")), digits = 0), 90)
                           
                        

exclude_sql = "
SELECT DISTINCT
a.[AccountNumber]
,cr.[EDW Customer ID] -- changed 8.30 from IBSCustomerID
,cr.[HH Durable Key]
,cr.[H_CIS_Household_Number]
FROM [EDWLoansandDeposits].[dbo].[Accounts] a
inner join [EDWLoansandDeposits].[dbo].[Products] p
on a.Product_ID=p.Product_ID
and p.[ProductType_ID] in (2,6) --Product_ID in ('178', '208', '250', '283')
and p.IsConsumerProductYN = 1
inner Join [EDW_Sandbox].[dbo].[Customer_Analytics_XRef_TEST]cr
ON RIGHT('00000000000' + CAST(cr.[Account Number] AS Varchar),11) = CAST(a.AccountNumber AS varchar)
AND cr.[Full Date] = %s
--and cr.[Open Closed Acct Code] = 'O'
where (a.[ClosedDate] between %s and %s OR a.ClosedDate is null)
and a.[Datasource_ID] = 1
"

#Apply fulldate string to SQL statement
exclude_sql_string = sprintf(exclude_sql, M4_full_date_string, M4_first_day_string, M3_last_day_string)

#Starter pop is HHs that opened a qualifying checking account in M3
exclude = DBI::dbGetQuery(dbicon, exclude_sql_string)


exclude2_sql = "
SELECT DISTINCT
a.[AccountNumber]
,a.[AccountOpenDate]
,a.[ClosedDate]
,cr.[EDW Customer ID] -- changed 8.30 from IBSCustomerID
,cr.[HH Durable Key]
,cr.[H_CIS_Household_Number]
,cr.[Full Date]
FROM [EDWLoansandDeposits].[dbo].[Accounts] a
inner join [EDWLoansandDeposits].[dbo].[Products] p
on a.Product_ID=p.Product_ID
and p.[ProductType_ID] in (2,6) --Product_ID in ('178', '208', '250', '283')
and p.IsConsumerProductYN = 1
inner Join [EDW_Sandbox].[dbo].[Customer_Analytics_XRef_TEST]cr
ON RIGHT('00000000000' + CAST(cr.[Account Number] AS Varchar),11) = CAST(a.AccountNumber AS varchar)
AND cr.[Full Date] = %s
and cr.[Open Closed Acct Code] = 'O'
where (a.[AccountOpenDate] <= %s)
and a.[Datasource_ID] = 1
"

#Apply fulldate string to SQL statement
exclude2_sql_string = sprintf(exclude2_sql, M3_full_date_string, M4_last_day_string)

#Starter pop is HHs that opened a qualifying checking account in M3
exclude2 = DBI::dbGetQuery(dbicon, exclude2_sql_string)

exclude1_hhs = c(unique(exclude$`HH Durable Key`))
exclude1_hhs_string = paste("'",as.character(exclude1_hhs),"'",collapse=", ",sep="")

exclude2_hhs = c(unique(exclude2$`HH Durable Key`))
exclude2_hhs_string = paste("'",as.character(exclude2_hhs),"'",collapse=", ",sep="")

inelig_hhs = c(exclude2_hhs, exclude1_hhs)

qual_hhs = c(unique(qual_dda$`HH Durable Key`))
elig_hhs = qual_hhs[which(!qual_hhs %in% inelig_hhs)]
qual_dda_no_prior = filter(qual_dda,  `HH Durable Key` %in% elig_hhs)

exclude1_cust = c(unique(exclude$`EDW Customer ID`))
exclude1_cust_string = paste("'",as.character(exclude1_cust),"'",collapse=", ",sep="")
exclude2_cust = c(unique(exclude2$`EDW Customer ID`))
exclude2_cust_string = paste("'",as.character(exclude2_cust),"'",collapse=", ",sep="")
inelig_cust = c(exclude1_cust, exclude2_cust)

qual_cust = c(unique(qual_dda_no_prior$`EDW Customer ID`))
elig_cust = qual_cust[which(!qual_cust %in% inelig_cust)]
qual_dda_no_prior = filter(qual_dda_no_prior,  `EDW Customer ID` %in% elig_cust)

qual_dda_no_prior = qual_dda_no_prior %>%  group_by(`HH Durable Key`) %>%filter(row_number() < 2)
qual_dda_no_prior = inner_join(M3_starting_df, qual_dda_no_prior, by=c("HH Durable Key"))

qual_hhs = c(unique(qual_dda_no_prior$`HH Durable Key`))
qual_hhs_string = paste("'",as.character(qual_hhs),"'",collapse=", ",sep="")

qual_acts = c(unique(qual_dda_no_prior$`AccountNumber`))
qual_acts_string = paste("'",as.character(qual_acts),"'",collapse=", ",sep="")

qual_custs = c(unique(qual_dda_no_prior$`EDW Customer ID`))
qual_custs_string = paste("'",as.character(qual_custs),"'",collapse=", ",sep="")

#Population Scrubbing

#Strip Primary-Trustee Aberation 
trustee_sql = "
SELECT  [HH Durable Key]
,[Account Relationship Type Desc]
,[EDW Customer ID] -- changed 8.30 from IBSCustomerID 
,[Account Number]
,[Full Date]
FROM [EDW_Sandbox].[dbo].[Customer_Analytics_XRef_TEST]
where [HH Durable Key] in (%s)
and [Full Date] = %s
and [Account Number] in (%s)
Group BY  [HH Durable Key]
,[Account Relationship Type Desc]
,[EDW Customer ID]
,[Account Number]
,[Full Date]
"
trustee_sql_string = sprintf(trustee_sql, qual_hhs_string, M3_full_date_string, qual_acts_string)
prime_trust = DBI::dbGetQuery(dbicon, trustee_sql_string)
#Filter to the root issue regarding Inidivdual/ trust rel
prime_trust = filter(prime_trust, `Account Relationship Type Desc` %in% c("INDIVIDUAL", "TRUST", "TRUSTEE"))
#Change how the data is viewed
primetrust_sum = prime_trust %>% group_by(`HH Durable Key`, `Account Number`) %>% 
  summarize(type = paste(sort(unique(`Account Relationship Type Desc`)),collapse=", "))
#Isolate combo relationships
primetrust_combo = filter(primetrust_sum, `type` %in% c("INDIVIDUAL, TRUST, TRUSTEE", "INDIVIDUAL, TRUST", "INDIVIDUAL, TRUSTEE", "TRUST, TRUSTEE"))

#Cast these HHs to a list, we will exclude from the main pop
prime_trust_hhs = c(unique(primetrust_combo$`HH Durable Key`))
prime_trust_string = paste("'",as.character(prime_trust_hhs),"'",collapse=", ",sep="")

#Identify HHs that closed their only DDA the month before our target
dda_closed_month_prior_sql = "
select [HH Durable Key]
FROM [EDW_Sandbox].[dbo].[Number_Of_Services_HST] 
WHERE [Full Date] = %s
AND [DDA] = 1
AND [Line of Business] = 'Retail Banking'
And [HH Durable Key] in (%s)
"
#This is all HH that had a DDA in M5 (2 months prior to target Month)
dda_closed_month_prior_sql_string = sprintf(dda_closed_month_prior_sql, M5_full_date_string, qual_hhs_string)
dda_closed = DBI::dbGetQuery(dbicon, dda_closed_month_prior_sql_string)
#keep just the HH Keys as string data
dda_closed_month_prior = c(unique(dda_closed$`HH Durable Key`))
dda_closed_month_prior_string = paste("'",as.character(dda_closed_month_prior),"'",collapse=", ",sep="")

#These are the HHs that had no DDA in M4 (1 month prior ro target Month). 
#I'm limiting the query to HHs that did have a DDA in M5.
#The result should be HHs that closed their only DDA the month prior to the target Month.
dda_closed_month_prior_sql2 = "
select [HH Durable Key]
FROM [EDW_Sandbox].[dbo].[Number_Of_Services_HST] 
WHERE [Full Date] = %s
AND [DDA] = 0
AND [Line of Business] = 'Retail Banking'
And [HH Durable Key] in (%s)
"
dda_closed_month_prior_sql_string2 = sprintf(dda_closed_month_prior_sql2, M4_full_date_string, dda_closed_month_prior_string)
dda_closed2 = DBI::dbGetQuery(dbicon, dda_closed_month_prior_sql_string2)
#Keep just the keys
dda_closed_month_prior2 = c(unique(dda_closed2$`HH Durable Key`))

#Quick Filter to remove HHs with DDA in prior Month
prior_dda_sql = "
select [HH Durable Key]
FROM [EDW_Sandbox].[dbo].[Number_Of_Services_HST]
WHERE [Full Date] = %s
AND [DDA] = 1
AND [Line of Business] = 'Retail Banking'
"

prior_dda_sql_string = sprintf(prior_dda_sql, M4_full_date_string)
prior_dda = DBI::dbGetQuery(dbicon, prior_dda_sql_string)

#Combine ineligible HH Lists. List will contain HHs that closed their only DDA the month prior, have an active DDA, 
#or were erroneous inclusions due to the trust/trustee relationship duplication 
pdda_hhs = c(unique(prior_dda$`HH Durable Key`))
inelig_hhs = c(pdda_hhs, prime_trust_hhs, dda_closed_month_prior2)#, inelig_hhs)#, dda_closed_month_prior2 ) # 
inelig_hhs_string = paste("'",as.character(inelig_hhs),"'",collapse=", ",sep="")

#Now take the ineligible list and subtract it from our qualified HHs to get the true pop.
elig_hhs = qual_hhs[which(!qual_hhs %in% inelig_hhs)]
hh_acct_open = filter(qual_dda_no_prior,  `HH Durable Key` %in% elig_hhs)

#Break out for later use and sub table writing.
hh_acct_open = hh_acct_open[c("HH Durable Key", "AccountOpenDate", "ClosedDate", "AccountNumber", "AverageLedgerBalanceMTD",
                                   "EDW Customer ID", "New HH This Month", "AccountClosed", "DaysOpen", "Associate ID", "Store Number")]
hh_acct_open$`Report Month` = paste0(months(M3_full_date, abbr = FALSE), " Month 3")


hh_acct_list = c(unique(hh_acct_open$AccountNumber))
hh_acct_list_string = paste("'",as.character(hh_acct_list),"'",collapse=", ",sep="")
hh_cust_list = c(unique(hh_acct_open$`EDW Customer ID`))
hh_cust_list_string = paste("'",as.character(hh_cust_list),"'",collapse=", ",sep="")

just_hh_durable_keys = hh_acct_open["HH Durable Key"] #changed here 9.3.19
just_hh_durable_keys = unique(just_hh_durable_keys)
just_hh_durable_keys_list =  c(unique(just_hh_durable_keys$`HH Durable Key`))
just_hh_durable_keys_list_string = paste("'",as.character(just_hh_durable_keys_list),"'",collapse=", ",sep="")


#Validate Inital Population ####
#Code to validate population. Keep commented out unless needed.

# #
# pop_val = read.delim("Y:/Zack/number_of_services_validation/New folder/pop_val_Oct.txt")
# pop_val = pop_val["H.HH.Durable.Key"]
# pop_val$H.HH.Durable.Key = as.numeric(pop_val$H.HH.Durable.Key)
# 
# false_negatives = anti_join(pop_val, just_hh_durable_keys, by=c("H.HH.Durable.Key" = "HH Durable Key"))
# print(nrow(false_negatives))
# false_negatives_may = inner_join(M3_starting_df, false_negatives, by=c("HH Durable Key" = "H.HH.Durable.Key"))
# #test = read.csv("Y:/Zack/og_false_pos.csv")
# #test = anti_join(false_negatives_may, durable_keys_to_remove, by=c("HH Durable Key"))
# 
# false_positives = anti_join(just_hh_durable_keys, pop_val, by=c("HH Durable Key" = "H.HH.Durable.Key"))
# print(nrow(false_positives))
# false_positives_may = inner_join(M3_starting_df, false_positives, by=c("HH Durable Key" = "HH Durable Key"))
# false_positives_may = inner_join(false_positives_may, qual_dda, by=c("HH Durable Key"))

#Building the M3 Report ##########################################################################################
#The next several sections are independant queries to find eligible cross sold products.
#No changes are required.
# M3 CD Products##########################################################################################
cd_products_sql = "
select distinct
a.[AccountNumber]
,a.[AccountOpenDate]
,ca.[Customer_ID]
--,cr.[HH Durable Key]
,cr.[Account Relationship Type Desc]
FROM [EDWLoansandDeposits].[dbo].[Accounts] a
inner join [EDWLoansandDeposits].[dbo].[CustomerAccounts] ca
on a.Account_ID = ca.Account_ID
inner join [EDWLoansandDeposits].[dbo].[Products] p
on a.Product_ID = p.Product_ID
inner join [EDW_Sandbox].[dbo].[Customer_Analytics_XRef_TEST] cr
on cr.[EDW Customer ID] = ca.[Customer_ID]
and cr.[Account Relationship Type Desc] in ('INDIVIDUAL', 'JOINT (OR)')
AND cr.[Full Date] = %s
where a.[AccountOpenDate] between %s and %s
and a.Product_ID in ('188', '190',	'193',	'226',	'238', '243',	'255')
AND (a.[ClosedDate] > %s  OR a.ClosedDate is null)
"
#Apply fulldate string to SQL statement
cd_products_sql_string = sprintf(cd_products_sql, M1_full_date_string, M3_first_day_string, M1_last_day_string, M1_last_day_string)

#Starter pop is HHs that opened a qualifying checking account in M3
cd_products = DBI::dbGetQuery(dbicon, cd_products_sql_string)
cd_products = inner_join(cd_products, hh_acct_open, by=c("Customer_ID" = "EDW Customer ID"))

# M3 Savings Products##########################################################################################
savings_products_sql = "
select distinct
a.[AccountNumber]
,a.[AccountOpenDate]
,ca.[Customer_ID]
--,cr.[HH Durable Key]
,cr.[Account Relationship Type Desc]
FROM [EDWLoansandDeposits].[dbo].[Accounts] a
inner join [EDWLoansandDeposits].[dbo].[CustomerAccounts] ca
on a.Account_ID = ca.Account_ID
inner join [EDWLoansandDeposits].[dbo].[Products] p
on a.Product_ID = p.Product_ID
inner join [EDW_Sandbox].[dbo].[Customer_Analytics_XRef_TEST] cr
on cr.[EDW Customer ID] = ca.[Customer_ID]
and cr.[Account Relationship Type Desc] in ('INDIVIDUAL', 'JOINT (OR)')
AND cr.[Full Date] = %s
where a.[AccountOpenDate] between %s and %s
and a.Product_ID in ('189',	'200',	'243',	'248',	'258')
AND (a.[ClosedDate] > %s  OR a.ClosedDate is null)
"

saving_products_sql_string = sprintf(savings_products_sql, M1_full_date_string, M3_first_day_string, M1_last_day_string, M1_last_day_string)
saving_products = DBI::dbGetQuery(dbicon, saving_products_sql_string)
saving_products = inner_join(saving_products, hh_acct_open, by=c("Customer_ID" = "EDW Customer ID"))

# M3 MM Products##########################################################################################
mm_products_sql = "
select distinct
a.[AccountNumber]
,p.ProductName
,a.[AccountOpenDate]
,ca.[Customer_ID]
--,cr.[HH Durable Key]
--,cr.[Account Relationship Type Desc]
FROM [EDWLoansandDeposits].[dbo].[Accounts] a
inner join [EDWLoansandDeposits].[dbo].[CustomerAccounts] ca
on a.Account_ID = ca.Account_ID
inner join [EDWLoansandDeposits].[dbo].[Products] p
on a.Product_ID = p.Product_ID
inner join [EDW_Sandbox].[dbo].[Customer_Analytics_XRef_TEST] cr
on cr.[EDW Customer ID] = ca.[Customer_ID]
and cr.[Account Relationship Type Desc] in ('INDIVIDUAL', 'JOINT (OR)')
AND cr.[Full Date] = %s
where a.[AccountOpenDate] between %s and %s
and a.Product_ID in ('175', '202',	'222',	'240',	'244',	'246',	'284')
AND (a.[ClosedDate] > %s  OR a.ClosedDate is null)
"
mm_products_sql_string = sprintf(mm_products_sql, M1_full_date_string, M3_first_day_string, M1_last_day_string, M1_last_day_string)
mm_products = DBI::dbGetQuery(dbicon, mm_products_sql_string)
mm_products = inner_join(mm_products, hh_acct_open, by=c("Customer_ID" = "EDW Customer ID"))
mm_products = unique(mm_products)
# M3 Loan Products##########################################################################################

loan_sql = "
select distinct
a.[AccountNumber]
,a.[AccountOpenDate]
,ca.[Customer_ID]
--,cr.[HH Durable Key]
--,cr.[Account Relationship Type Desc]
FROM [EDWLoansandDeposits].[dbo].[Accounts] a
inner join [EDWLoansandDeposits].[dbo].[CustomerAccounts] ca
on a.Account_ID = ca.Account_ID
inner join [EDWLoansandDeposits].[dbo].[Products] p
on a.Product_ID = p.Product_ID
inner join [EDWLoansandDeposits].[dbo].[LoansMonthly] l
on a.Account_ID = l.Account_ID
inner join [EDW_Sandbox].[dbo].[Customer_Analytics_XRef_TEST] cr
on cr.[EDW Customer ID] = ca.[Customer_ID]
and cr.[Account Relationship Type Desc] in ('INDIVIDUAL', 'JOINT (OR)')
AND cr.[Full Date] = %s
where l.ReportingGroup_ID in (12,13,14,15,16)
and l.DateKey between %s and %s
and a.[AccountOpenDate] between %s and %s
and a.Product_ID in (11, 25, 26, 280)
AND (a.[ClosedDate] > %s  OR a.ClosedDate is null)
"
loan_sql_string = sprintf(loan_sql, M1_full_date_string, M3_first_day_string, M1_last_day_string, M3_first_day_string, M1_last_day_string, M1_last_day_string)
loans = DBI::dbGetQuery(dbicon, loan_sql_string)
loans = inner_join(loans, hh_acct_open, by=c("Customer_ID" = "EDW Customer ID"))
loans = unique(loans)
# M3 Mortage Products##########################################################################################

mort_sql = "
select distinct
a.[AccountNumber]
,a.[AccountOpenDate]
,ca.[Customer_ID]
,cr.[Account Relationship Type Desc]
FROM [EDWLoansandDeposits].[dbo].[Accounts] a
inner join [EDWLoansandDeposits].[dbo].[CustomerAccounts] ca
on a.Account_ID = ca.Account_ID
inner join [EDWLoansandDeposits].[dbo].[Products] p
on a.Product_ID = p.Product_ID
inner join [EDW_Sandbox].[dbo].[Customer_Analytics_XRef_TEST] cr
on cr.[EDW Customer ID] = ca.[Customer_ID]
and cr.[Account Relationship Type Desc] in ('INDIVIDUAL', 'JOINT (OR)')
AND cr.[Full Date] = %s
where p.Product_ID = 12
and a.[AccountOpenDate] between %s and %s
AND (a.[ClosedDate] > %s  OR a.ClosedDate is null)
"
mort_sql_string = sprintf(mort_sql, M1_full_date_string, M3_first_day_string, M1_last_day_string, M1_last_day_string)
mortgage = DBI::dbGetQuery(dbicon, mort_sql_string)
mortgage = inner_join(mortgage, hh_acct_open, by=c("Customer_ID" = "EDW Customer ID"))
mortgage = unique(mortgage)

# M3 Digital and Debit ##########################################################################################
q2_sql = "
SELECT distinct
a.[Customer_ID]
,cr.[HH Durable Key]
,a.[Service_ID]
,a.[CreateDate]
,cr.[Account Open Date]
FROM [EDWLoansandDeposits].[dbo].[CustomerServices] a
inner join [EDW_Sandbox].[dbo].[Customer_Analytics_XRef_TEST] cr
on cr.[EDW Customer ID] = a.[Customer_ID]
--where cr.[Account Relationship Type Desc] in ('INDIVIDUAL', 'JOINT (OR)')
--and cr.[Acct Prod Product ID] in ('OSTMT', 'QDCX', 'QMOBI', 'QBPAY', 'DIRDP', 'SB', 'VD')
where cr.[Full Date] >= %s
--and cr.[Account Open Date] >= %s
--and cr.[Account Number] in (%s)
"
q2_products_sql_string = sprintf(q2_sql, M3_full_date_string, M3_first_day_string, hh_acct_list_string)#, M3_last_day_string)
q2_products = DBI::dbGetQuery(dbicon, q2_products_sql_string)
q2_products$CreateDate = ymd_hms(q2_products$CreateDate)
q2_products = filter(q2_products, CreateDate >= M3_first_day)
q2_products = unique(q2_products)

#Product Break Out
Bill_Pay = filter(q2_products, Service_ID == 7)
Debit_Card = filter(q2_products, Service_ID == 21)
Direct_Deposit = filter(q2_products, Service_ID == 22)
Mobile_Deposit = filter(q2_products, Service_ID == 51)
Safe_Box = filter(q2_products, Service_ID == 38)
#Online_Banking = filter(q2_products, Service_ID == 49)
#GoTo = filter(q2_products, Service_ID == 50)
Online_Statement = filter(q2_products, Service_ID == 56)


go_olb_sql = "
SELECT distinct
[EDW Customer ID]
,[HH Durable Key]
,[Account Open Date]
,[Acct Prod Product ID]
,[Acct Prod Product Desc]
FROM [EDW_Sandbox].[dbo].[Customer_Analytics_XRef_TEST]
where [Full Date] >= %s
and [Acct Prod Product ID] in ('QDCX', 'GOTO')
and [Account Open Date] between %s and %s
and [Open Closed Acct Code] = 'O'
"
go_olb_sql_string = sprintf(go_olb_sql, M3_full_date_string, M3_first_day_string, M1_last_day_string)
go_olb_products = DBI::dbGetQuery(dbicon, go_olb_sql_string)

Online_Banking = filter(go_olb_products, `Acct Prod Product ID` == "QDCX")
Online_Banking = inner_join(Online_Banking, hh_acct_open, by=c("EDW Customer ID"))
Online_Banking = unique(Online_Banking)

GoTo = filter(go_olb_products, `Acct Prod Product ID` == "GOTO")
GoTo = inner_join(GoTo, hh_acct_open, by=c("EDW Customer ID"))
GoTo = unique(GoTo)

# M3 Credit Cards##########################################################################################

cc_sql = "
select distinct * 
FROM [EDW_Sandbox].[dbo].[Customer_Analytics_XRef_TEST]
where [Acct Prod Product ID] in ('01', '17')
and [Account Open Date] between %s and %s
"
credit_cards_string = sprintf(cc_sql, M3_first_day_string, M1_last_day_string)
credit_cards = DBI::dbGetQuery(dbicon, credit_cards_string)
credit_cards = inner_join(credit_cards, hh_acct_open, by=c("HH Durable Key"))
credit_cards = unique(credit_cards)


# M3 Cash Cards##########################################################################################

cash_card_sql = "
select distinct
a.[AccountNumber]
,a.[PrimaryCustomer_ID]
,a.[LastReceivedDate]
FROM [EDWLoansandDeposits].[dbo].[Accounts] a
where a.[LastReceivedDate] between %s and %s
and a.Product_ID in ('275', '276', '277')
"
cash_card_sql_string = sprintf(cash_card_sql, M3_first_day_string, M1_last_day_string)#, M3_last_day_string)
cash_cards = DBI::dbGetQuery(dbicon, cash_card_sql_string)
cash_cards = inner_join(cash_cards, hh_acct_open, by=c("PrimaryCustomer_ID" = "EDW Customer ID"))
cash_cards = unique(cash_cards)

# Create Fields for Final M3 DataFrame ##########################################################################################

M3_final_frame = just_hh_durable_keys["HH Durable Key"]
M3_final_frame = inner_join(M3_final_frame, M3_starting_df, by=c("HH Durable Key"))
M3_final_frame = M3_final_frame[-c(3)]
M3_final_frame = inner_join(M3_final_frame, hh_acct_open, by=c("HH Durable Key"))
#M3_final_frame = filter(M3_final_frame, Region != "Digital and Central Banking")
#M3_final_frame = M3_final_frame[,-c(2, 8, 10)]
# Remove Dupes
M3_final_frame =M3_final_frame %>%  group_by(`HH Durable Key`) %>%filter(row_number() < 2)

M3_final_frame$`Report Month` = paste0(months(M3_full_date, abbr = FALSE), " Month 3_XREF")

#Create DDA Yes/No
M3_final_frame$DDA = ifelse(is.na(M3_final_frame$`ClosedDate`) | (M3_final_frame$`ClosedDate` > M1_last_day),1,0)
#Create CD Yes/ No
M3_final_frame$CDs = ifelse(M3_final_frame$`HH Durable Key` %in% cd_products$`HH Durable Key`,
                            ifelse(cd_products$AccountOpenDate.x >= cd_products$AccountOpenDate.y, 1, 0), 0)
#Create Savings Yes/ No
M3_final_frame$Savings = ifelse(M3_final_frame$`HH Durable Key` %in% saving_products$`HH Durable Key`,
                                ifelse(saving_products$AccountOpenDate.x >= saving_products$AccountOpenDate.y, 1, 0), 0)

#Create MM Yes/ No
M3_final_frame$MM = ifelse(M3_final_frame$`HH Durable Key` %in% mm_products$`HH Durable Key`,
                           ifelse(mm_products$AccountOpenDate.x >= mm_products$AccountOpenDate.y, 1, 0), 0)                            

#Create Loan Yes/ No
M3_final_frame$`Loan` = ifelse(M3_final_frame$`HH Durable Key` %in% loans$`HH Durable Key`,
                                         ifelse(loans$AccountOpenDate.x >= loans$AccountOpenDate.y, 1, 0), 0)

#Create Mortgage Yes/ No
M3_final_frame$`Mortgage` = ifelse(M3_final_frame$`HH Durable Key` %in% mortgage$`HH Durable Key`,
                                         ifelse(mortgage$AccountOpenDate.x >= mortgage$AccountOpenDate.y, 1, 0), 0)

#Create Online Banking Yes/ No
M3_final_frame$`Online Banking` = ifelse(M3_final_frame$`HH Durable Key` %in% Online_Banking$`HH Durable Key`, 1, 0)

#Create GOTO Yes/ No
M3_final_frame$GOTO = ifelse(M3_final_frame$`HH Durable Key` %in% GoTo$`HH Durable Key`, 1, 0)

#Create E Statment  Yes/ No
M3_final_frame$`Online Statements` = ifelse(M3_final_frame$`HH Durable Key` %in% Online_Statement$`HH Durable Key`, 1, 0)

#Create Debit  Yes/ No
M3_final_frame$`Debit Card` = ifelse(M3_final_frame$`HH Durable Key` %in% Debit_Card$`HH Durable Key`, 1, 0)

#Create Credit  Yes/ No
M3_final_frame$`Credit Card` = ifelse(M3_final_frame$`HH Durable Key` %in% credit_cards$`HH Durable Key`,
                               ifelse(credit_cards$`Account Open Date` >= credit_cards$AccountOpenDate, 1, 0), 0)
#Create Cash Card  Yes/ No
M3_final_frame$`Cash Card` = ifelse(M3_final_frame$`HH Durable Key` %in% cash_cards$`HH Durable Key`, 1, 0)
   
#Create DD  Yes/ No
M3_final_frame$`Direct Deposit` = ifelse(M3_final_frame$`HH Durable Key` %in% Direct_Deposit$`HH Durable Key`, 1, 0)

#Create DD  Yes/ No
M3_final_frame$`Bill Pay` = ifelse(M3_final_frame$`HH Durable Key` %in% Bill_Pay$`HH Durable Key`, 1, 0)

#Create DD  Yes/ No
M3_final_frame$`Mobile` = ifelse(M3_final_frame$`HH Durable Key` %in% Mobile_Deposit$`HH Durable Key`, 1, 0)

#Create DD  Yes/ No
M3_final_frame$`Safe Box` = ifelse(M3_final_frame$`HH Durable Key` %in% Safe_Box$`HH Durable Key`, 1, 0)

M3_final_frame$`Total Services` = rowSums(M3_final_frame[,15:30])

M3_final_frame$`Total CON Services` = rowSums(M3_final_frame[,15:20])
M3_final_frame$Drop = ifelse(M3_final_frame$AccountClosed == "Closed",
                             ifelse(M3_final_frame$`Total CON Services` == 0, "drop", "keep"), "keep")

M3_final_frame = filter(M3_final_frame, Drop == "keep")
M3_final_frame = M3_final_frame[,-c(32, 33)]
# XSell History Table for Dashboard ####
xsell_summary = M3_final_frame %>% 
  group_by(`Associate ID`) %>% 
  summarise(`Cross Sell` = round(sum(`Total Services`)/n(), digits = 2)
  )
xsell_summary$`Full Date` = M3_full_date

# Product Penetration History Table for Dashboard ####
prod_summary = M3_final_frame %>% 
  group_by(`Associate ID`) %>% 
  summarise(`DDA` = sum(DDA),`CDs` = sum(CDs), `Savings` = sum(Savings), 
            `MM` = sum(MM),`Loan` = sum(Loan), `Mortgage` = sum(Mortgage),
            `Online Banking` = sum(`Online Banking`), `Bill Pay` = sum(`Bill Pay`),
            `Online Statements` = sum(`Online Statements`), `Mobile` = sum(`Mobile`),
            `Debit` = sum(`Debit Card`), `Credit` = sum(`Credit Card`), `Cash Card` = sum(`Cash Card`),
            `Direct Deposit` = sum(`Direct Deposit`), `Safe Box` = sum(`Safe Box`), GOTO = sum(`GOTO`)
  )
prod_summary$`Full Date` = M3_full_date

# test_summary = M3_final_frame %>% 
#   group_by(`Report Month`) %>% 
#   summarise(`DDA` = sum(DDA),`CDs` = sum(CDs), `Savings` = sum(Savings), 
#             `MM` = sum(MM),`Loan` = sum(Loan), `Mortgage` = sum(Mortgage),
#             `Online Banking` = sum(`Online Banking`), `Bill Pay` = sum(`Bill Pay`),
#             `Online Statements` = sum(`Online Statements`), `Mobile` = sum(`Mobile`),
#             `Debit` = sum(`Debit Card`), `Credit` = sum(`Credit Card`), `Cash Card` = sum(`Cash Card`),
#             `Direct Deposit` = sum(`Direct Deposit`), `Safe Box` = sum(`Safe Box`), GOTO = sum(`GOTO`)
#   )

#M3 Output Writing ##########################################################################################
#Write table to history databases
dbWriteTable(dbicon, name = "NRD_DDA_HST", hh_acct_open, row.names = FALSE, append=TRUE)

#Write XSell History Table to Sandbox
dbWriteTable(dbicon, name = "NRD_XSell_Sum_HST", xsell_summary, row.names = FALSE, append=TRUE)

#Write Full Report History Table to Sandbox
dbWriteTable(dbicon, name = "NRD_HST", M3_final_frame, row.names = FALSE, append=TRUE)

#Product Penetration History Table to Sandbox
dbWriteTable(dbicon, name = "NRD_Prod_Sum_HST", prod_summary, row.names = FALSE, append=TRUE)
# Flat File HSS Output for Spark
g = as.Date(M3_full_date)
g = format(g,"%m %b")

FileYear = as.Date(M3_full_date)
FileYear = format(FileYear,"%Y")

store_report = M3_final_frame %>% group_by(`Store Number`) %>% summarise(`Total HHs` = n(),
                                                                                       `# Serv` = sum(`Total Services`))
store_report$Date = M3_full_date
store_report = store_report[c(4,1,2,3)]
store_report = filter(store_report, `Store Number` %in% c('50231',	'50163',	'50418',	'50205',	'50409',	'50229',	'50408',	'50232',	'50160',	'50134',	'50411',	'50412',	'50162',	'50161',	'50234',	'50402',	'50383',	'50410',	'50159',	'50401',	'50416',	'50417',	'50093',	'50206',	'50095',	'50201',	'50125',	'50102',	'50200',	'50151',	'50123',	'50126',	'50085',	'50101',	'50203',	'50118',	'50097',	'50117',	'50199',	'50127',	'50149',	'50120',	'50094',	'59011',	'50996',	'59009',	'50089',	'50100',	'50075',	'50079',	'50096',	'50087',	'50078',	'50103',	'50092',	'50076',	'50091',	'50081',	'50077',	'50088',	'50098',	'50083',	'50090',	'50080',	'50141',	'50132',	'50116',	'50145',	'50137',	'50122',	'50215',	'50135',	'50131',	'50115',	'50143',	'50112',	'50130',	'50140',	'50124',	'50136',	'50113',	'50144',	'50139',	'50138',	'50377',	'50227',	'50373',	'50157',	'50374',	'50381',	'50376',	'50155',	'50372',	'50225',	'50384',	'50110',	'50111',	'50379',	'50230',	'50378',	'50082',	'50154',	'50156',	'50107',	'50207',	'50995',	'59008',	'50247',	'50289',	'50361',	'50319',	'50238',	'50275',	'50277',	'50278',	'50366',	'50349',	'50369',	'50280',	'50363',	'50274',	'50276',	'50281',	'50364',	'50403',	'50237',	'50272',	'50302',	'50404',	'50341',	'50279',	'50386',	'50354',	'50371',	'50299',	'50294',	'50235',	'50273',	'50271',	'50343',	'50300',	'50241',	'50288',	'50365',	'50285',	'50242',	'50296',	'50287',	'50329',	'50250',	'50243',	'50239',	'50286',	'50413',	'50325',	'50314',	'50249',	'50336',	'50245',	'50244',	'50321',	'50368',	'50298',	'50337',	'50251',	'50389',	'50065',	'50393',	'50174',	'50261',	'50391',	'50169',	'50392',	'50177',	'50397',	'50390',	'50223',	'50394',	'50388',	'50068',	'50061',	'50396',	'50347',	'50059',	'50322',	'50224',	'50346',	'50055',	'50262',	'50070',	'50414',	'50165',	'50211',	'50014',	'50260',	'50150',	'50071',	'50213',	'50342',	'50060',	'50073',	'50152',	'50062',	'50056',	'50104',	'50208',	'50072',	'50067',	'50210',	'50057',	'50997',	'59006',	'50370',	'50074',	'50292',	'50395',	'50340',	'50327',	'50330',	'50109',	'50324',	'50367',	'50167',	'50301',	'50166',	'50311',	'50338',	'50196',	'50219',	'50350',	'50252',	'50192',	'50184',	'50406',	'50195',	'50355',	'50351',	'50265',	'50291',	'50269',	'50248',	'50362',	'50188',	'50246',	'50266',	'50284',	'50303',	'50178',	'50297',	'50344',	'50293',	'50221',	'50218',	'50387',	'50182',	'50222',	'50415',	'50267',	'50214',	'50220',	'50998',	'59007',	'50187',	'50254',	'50240',	'50191',	'50255',	'50197',	'50253',	'50256',	'50185',	'50193',	'50295',	'50190',	'50263',	'50259',	'50194',	'50290',	'50264',	'50258',	'50186',	'50189',	'50257',	'50019',	'50308',	'50023',	'50026',	'50316',	'50017',	'50003',	'50306',	'50027',	'50334',	'50024',	'50018',	'50307',	'50028',	'50312',	'50002',	'50015',	'50317',	'50328',	'50041',	'50000',	'50035',	'50020',	'50037',	'50356',	'50038',	'50008',	'50320',	'50001',	'50339',	'50045',	'50315',	'50039',	'50326',	'50036',	'50146',	'50005',	'50006',	'50042',	'50999',	'59010',	'50063',	'50029',	'50053',	'50010',	'50051',	'50043',	'50030',	'50066',	'50283',	'50050',	'50032',	'50318',	'50047',	'50044',	'50011',	'50013',	'50054',	'50033',	'50048',	'50052',	'50282'
) )

write.table(store_report, paste0(drive, ":/NRD/NRD_Output/", FileYear, "/", g, "/HSS.txt"), sep="\t", quote = FALSE, row.names = FALSE)

associate_report = M3_final_frame %>% group_by(`Associate ID`) %>% summarise(`Total HHs` = n(),
                                                                         `# Serv` = sum(`Total Services`))
associate_report$Date = M3_full_date
associate_report = associate_report[c(4,1,2,3)]
write.table(associate_report, paste0(drive, ":/NRD/NRD_Output/", FileYear, "/", g, "/Associate_NRD.txt"), sep="\t", quote = FALSE, row.names = FALSE)
# MONTH 2 Population Creation##########################################################################################
numb_svcs_M2_string = sprintf(numb_svcs, M2_full_date_string)
M2_starting_df = DBI::dbGetQuery(dbicon, numb_svcs_M2_string)
qual_dda_sql_string = sprintf(qual_dda_sql, M2_full_date_string, M2_first_day_string, M2_last_day_string, M2_last_day_string, M2_first_day_string,  M1_last_day_string)
qual_dda = DBI::dbGetQuery(dbicon, qual_dda_sql_string)
qual_dda$AccountClosed = ifelse((is.na(qual_dda$ClosedDate)| qual_dda$ClosedDate > M1_last_day), "Open", "Closed")
qual_dda$DaysOpen = ifelse(qual_dda$AccountClosed == "Closed", round(difftime(qual_dda$ClosedDate , qual_dda$AccountOpenDate, units = c("days")), digits = 0), 90)

exclude_sql_string = sprintf(exclude_sql, M3_full_date_string, M3_first_day_string, M2_last_day_string)
exclude = DBI::dbGetQuery(dbicon, exclude_sql_string)
exclude2_sql_string = sprintf(exclude2_sql, M2_full_date_string, M3_last_day_string)
exclude2 = DBI::dbGetQuery(dbicon, exclude2_sql_string)

exclude1_hhs = c(unique(exclude$`HH Durable Key`))
exclude1_hhs_string = paste("'",as.character(exclude1_hhs),"'",collapse=", ",sep="")
exclude2_hhs = c(unique(exclude2$`HH Durable Key`))
exclude2_hhs_string = paste("'",as.character(exclude2_hhs),"'",collapse=", ",sep="")
inelig_hhs = c(exclude2_hhs, exclude1_hhs)

qual_hhs = c(unique(qual_dda$`HH Durable Key`))
elig_hhs = qual_hhs[which(!qual_hhs %in% inelig_hhs)]
qual_dda_no_prior = filter(qual_dda,  `HH Durable Key` %in% elig_hhs)

exclude1_cust = c(unique(exclude$`EDW Customer ID`))
exclude1_cust_string = paste("'",as.character(exclude1_cust),"'",collapse=", ",sep="")
exclude2_cust = c(unique(exclude2$`EDW Customer ID`))
exclude2_cust_string = paste("'",as.character(exclude2_cust),"'",collapse=", ",sep="")
inelig_cust = c(exclude1_cust, exclude2_cust)

qual_cust = c(unique(qual_dda_no_prior$`EDW Customer ID`))
elig_cust = qual_cust[which(!qual_cust %in% inelig_cust)]
qual_dda_no_prior = filter(qual_dda_no_prior,  `EDW Customer ID` %in% elig_cust)

qual_dda_no_prior = qual_dda_no_prior %>%  group_by(`HH Durable Key`) %>%filter(row_number() < 2)
qual_dda_no_prior = inner_join(M2_starting_df, qual_dda_no_prior, by=c("HH Durable Key"))

qual_hhs = c(unique(qual_dda_no_prior$`HH Durable Key`))
qual_hhs_string = paste("'",as.character(qual_hhs),"'",collapse=", ",sep="")
qual_acts = c(unique(qual_dda_no_prior$`AccountNumber`))
qual_acts_string = paste("'",as.character(qual_acts),"'",collapse=", ",sep="")
qual_custs = c(unique(qual_dda_no_prior$`EDW Customer ID`))
qual_custs_string = paste("'",as.character(qual_custs),"'",collapse=", ",sep="")

trustee_sql_string = sprintf(trustee_sql, qual_hhs_string, M2_full_date_string, qual_acts_string)
prime_trust = DBI::dbGetQuery(dbicon, trustee_sql_string)
prime_trust = filter(prime_trust, `Account Relationship Type Desc` %in% c("INDIVIDUAL", "TRUST", "TRUSTEE"))
primetrust_sum = prime_trust %>% group_by(`HH Durable Key`, `Account Number`) %>% 
  summarize(type = paste(sort(unique(`Account Relationship Type Desc`)),collapse=", "))
primetrust_combo = filter(primetrust_sum, `type` %in% c("INDIVIDUAL, TRUST, TRUSTEE", "INDIVIDUAL, TRUST", "INDIVIDUAL, TRUSTEE", "TRUST, TRUSTEE"))
prime_trust_hhs = c(unique(primetrust_combo$`HH Durable Key`))
prime_trust_string = paste("'",as.character(prime_trust_hhs),"'",collapse=", ",sep="")

dda_closed_month_prior_sql_string = sprintf(dda_closed_month_prior_sql, M4_full_date_string, qual_hhs_string)
dda_closed = DBI::dbGetQuery(dbicon, dda_closed_month_prior_sql_string)
dda_closed_month_prior = c(unique(dda_closed$`HH Durable Key`))
dda_closed_month_prior_string = paste("'",as.character(dda_closed_month_prior),"'",collapse=", ",sep="")
dda_closed_month_prior_sql_string2 = sprintf(dda_closed_month_prior_sql2, M3_full_date_string, dda_closed_month_prior_string)
dda_closed2 = DBI::dbGetQuery(dbicon, dda_closed_month_prior_sql_string2)
dda_closed_month_prior2 = c(unique(dda_closed2$`HH Durable Key`))
prior_dda_sql_string = sprintf(prior_dda_sql, M3_full_date_string)
prior_dda = DBI::dbGetQuery(dbicon, prior_dda_sql_string)
#Combine ineligible HH Lists. List will contain HHs that closed their only DDA the month prior, have an active DDA, 
#or were erroneous inclusions due to the trust/trustee relationship duplication 
pdda_hhs = c(unique(prior_dda$`HH Durable Key`))
inelig_hhs = c(pdda_hhs, prime_trust_hhs, dda_closed_month_prior2)#, inelig_hhs)#, dda_closed_month_prior2 ) # 
inelig_hhs_string = paste("'",as.character(inelig_hhs),"'",collapse=", ",sep="")

#Now take the ineligible list and subtract it from our qualified HHs to get the true pop.
elig_hhs = qual_hhs[which(!qual_hhs %in% inelig_hhs)]
hh_acct_open = filter(qual_dda_no_prior,  `HH Durable Key` %in% elig_hhs)

#Break out for later use and sub table writing.
hh_acct_open = hh_acct_open[c("HH Durable Key", "AccountOpenDate", "ClosedDate", "AccountNumber", "AverageLedgerBalanceMTD",
                              "EDW Customer ID", "New HH This Month", "AccountClosed", "DaysOpen", "Associate ID", "Store Number")]
hh_acct_open$`Report Month` = paste0(months(M2_full_date, abbr = FALSE), " Month 2")


hh_acct_list = c(unique(hh_acct_open$AccountNumber))
hh_acct_list_string = paste("'",as.character(hh_acct_list),"'",collapse=", ",sep="")
hh_cust_list = c(unique(hh_acct_open$`EDW Customer ID`))
hh_cust_list_string = paste("'",as.character(hh_cust_list),"'",collapse=", ",sep="")

just_hh_durable_keys = hh_acct_open["HH Durable Key"] #changed here 9.3.19
just_hh_durable_keys = unique(just_hh_durable_keys)
just_hh_durable_keys_list =  c(unique(just_hh_durable_keys$`HH Durable Key`))
just_hh_durable_keys_list_string = paste("'",as.character(just_hh_durable_keys_list),"'",collapse=", ",sep="")

#Validate Inital Population ####
#Code to validate population. Keep commented out unless needed.

#
# pop_val = read.delim("Y:/Zack/number_of_services_validation/New folder/pop_val_Oct.txt")
# pop_val = pop_val["H.HH.Durable.Key"]
# pop_val$H.HH.Durable.Key = as.numeric(pop_val$H.HH.Durable.Key)
# 
# false_negatives = anti_join(pop_val, just_hh_durable_keys, by=c("H.HH.Durable.Key" = "HH Durable Key"))
# print(nrow(false_negatives))
# false_negatives_may = inner_join(M3_starting_df, false_negatives, by=c("HH Durable Key" = "H.HH.Durable.Key"))
# #test = read.csv("Y:/Zack/og_false_pos.csv")
# #test = anti_join(false_negatives_may, durable_keys_to_remove, by=c("HH Durable Key"))
# 
# false_positives = anti_join(just_hh_durable_keys, pop_val, by=c("HH Durable Key" = "H.HH.Durable.Key"))
# print(nrow(false_positives))
# false_positives_may = inner_join(M3_starting_df, false_positives, by=c("HH Durable Key" = "HH Durable Key"))
# false_positives_may = inner_join(false_positives_may, qual_dda, by=c("HH Durable Key"))
# M2 Final Frame ##########################################################################################
# M2 CD Products##########################################################################################

#Apply fulldate string to SQL statement
cd_products_sql_string = sprintf(cd_products_sql, M1_full_date_string, M2_first_day_string, M1_last_day_string, M1_last_day_string)

#Starter pop is HHs that opened a qualifying checking account in M3
cd_products = DBI::dbGetQuery(dbicon, cd_products_sql_string)
cd_products = inner_join(cd_products, hh_acct_open, by=c("Customer_ID" = "EDW Customer ID"))

# M2 Savings Products##########################################################################################

saving_products_sql_string = sprintf(savings_products_sql, M1_full_date_string, M2_first_day_string, M1_last_day_string, M1_last_day_string)
saving_products = DBI::dbGetQuery(dbicon, saving_products_sql_string)
saving_products = inner_join(saving_products, hh_acct_open, by=c("Customer_ID" = "EDW Customer ID"))

# M2 MM Products##########################################################################################

mm_products_sql_string = sprintf(mm_products_sql, M1_full_date_string, M2_first_day_string, M1_last_day_string, M1_last_day_string)
mm_products = DBI::dbGetQuery(dbicon, mm_products_sql_string)
mm_products = inner_join(mm_products, hh_acct_open, by=c("Customer_ID" = "EDW Customer ID"))
mm_products = unique(mm_products)
# M2 Loan Products##########################################################################################

loan_sql_string = sprintf(loan_sql, M1_full_date_string, M2_first_day_string, M1_last_day_string, M2_first_day_string, M1_last_day_string, M1_last_day_string)
loans = DBI::dbGetQuery(dbicon, loan_sql_string)
loans = inner_join(loans, hh_acct_open, by=c("Customer_ID" = "EDW Customer ID"))
loans = unique(loans)
# M2 Mortage Products##########################################################################################

mort_sql_string = sprintf(mort_sql, M1_full_date_string, M2_first_day_string, M1_last_day_string, M1_last_day_string)
mortgage = DBI::dbGetQuery(dbicon, mort_sql_string)
mortgage = inner_join(mortgage, hh_acct_open, by=c("Customer_ID" = "EDW Customer ID"))
mortgage = unique(mortgage)

# M2 Digital and Debit ##########################################################################################

q2_products_sql_string = sprintf(q2_sql, M2_full_date_string, hh_acct_list_string)#, M3_last_day_string)
q2_products = DBI::dbGetQuery(dbicon, q2_products_sql_string)
q2_products$CreateDate = ymd_hms(q2_products$CreateDate)
q2_products = filter(q2_products, CreateDate >= M2_first_day)

#Product Break Out
Bill_Pay = filter(q2_products, Service_ID == 7)
Debit_Card = filter(q2_products, Service_ID == 21)
Direct_Deposit = filter(q2_products, Service_ID == 22)
Mobile_Deposit = filter(q2_products, Service_ID == 51)
Safe_Box = filter(q2_products, Service_ID == 38)
#Online_Banking = filter(q2_products, Service_ID == 49)
#GoTo = filter(q2_products, Service_ID == 50)
Online_Statement = filter(q2_products, Service_ID == 56)

go_olb_sql_string = sprintf(go_olb_sql, M2_full_date_string, M2_first_day_string, M1_last_day_string)
go_olb_products = DBI::dbGetQuery(dbicon, go_olb_sql_string)

Online_Banking = filter(go_olb_products, `Acct Prod Product ID` == "QDCX")
GoTo = filter(go_olb_products, `Acct Prod Product ID` == "GOTO")

# M2 Credit Cards##########################################################################################

credit_cards_string = sprintf(cc_sql, M2_first_day_string, M1_last_day_string)
credit_cards = DBI::dbGetQuery(dbicon, credit_cards_string)
credit_cards = inner_join(credit_cards, hh_acct_open, by=c("HH Durable Key"))
credit_cards = unique(credit_cards)


# M2 Cash Cards##########################################################################################

cash_card_sql_string = sprintf(cash_card_sql, M2_first_day_string, M1_last_day_string)#, M3_last_day_string)
cash_cards = DBI::dbGetQuery(dbicon, cash_card_sql_string)
cash_cards = inner_join(cash_cards, hh_acct_open, by=c("PrimaryCustomer_ID" = "EDW Customer ID"))
cash_cards = unique(cash_cards)

#Create Fields for Final M2 DataFrame##########################################################################################

M2_final_frame = just_hh_durable_keys["HH Durable Key"]
M2_final_frame = inner_join(M2_final_frame, M2_starting_df, by=c("HH Durable Key"))
M2_final_frame = M2_final_frame[-c(3)]
M2_final_frame = inner_join(M2_final_frame, hh_acct_open, by=c("HH Durable Key"))
#M2_final_frame = filter(M2_final_frame, Region != "Digital and Central Banking")

# Remove Dupes
M2_final_frame =M2_final_frame %>%  group_by(`HH Durable Key`) %>%filter(row_number() < 2)

#Create DDA Yes/No
M2_final_frame$DDA = ifelse(is.na(M2_final_frame$`ClosedDate`) | (M2_final_frame$`ClosedDate` > M1_last_day),1,0)
#Create CD Yes/ No
M2_final_frame$CDs = ifelse(M2_final_frame$`HH Durable Key` %in% cd_products$`HH Durable Key`,
                            ifelse(cd_products$AccountOpenDate.x >= cd_products$AccountOpenDate.y, 1, 0), 0)
#Create Savings Yes/ No
M2_final_frame$Savings = ifelse(M2_final_frame$`HH Durable Key` %in% saving_products$`HH Durable Key`,
                                ifelse(saving_products$AccountOpenDate.x >= saving_products$AccountOpenDate.y, 1, 0), 0)

#Create MM Yes/ No
M2_final_frame$MM = ifelse(M2_final_frame$`HH Durable Key` %in% mm_products$`HH Durable Key`,
                           ifelse(mm_products$AccountOpenDate.x >= mm_products$AccountOpenDate.y, 1, 0), 0)                            

#Create Loan Yes/ No
M2_final_frame$`Loan` = ifelse(M2_final_frame$`HH Durable Key` %in% loans$`HH Durable Key`,
                               ifelse(loans$AccountOpenDate.x >= loans$AccountOpenDate.y, 1, 0), 0)

#Create Mortgage Yes/ No
M2_final_frame$`Mortgage` = ifelse(M2_final_frame$`HH Durable Key` %in% mortgage$`HH Durable Key`,
                                   ifelse(mortgage$AccountOpenDate.x >= mortgage$AccountOpenDate.y, 1, 0), 0)

#Create Online Banking Yes/ No
M2_final_frame$`Online Banking` = ifelse(M2_final_frame$`HH Durable Key` %in% Online_Banking$`HH Durable Key`, 1, 0)

#Create GOTO Yes/ No
M2_final_frame$GOTO = ifelse(M2_final_frame$`HH Durable Key` %in% GoTo$`HH Durable Key`, 1, 0)

#Create E Statment  Yes/ No
M2_final_frame$`Online Statements` = ifelse(M2_final_frame$`HH Durable Key` %in% Online_Statement$`HH Durable Key`, 1, 0)

#Create Debit  Yes/ No
M2_final_frame$`Debit Card` = ifelse(M2_final_frame$`HH Durable Key` %in% Debit_Card$`HH Durable Key`, 1, 0)

#Create Credit  Yes/ No
#M2_final_frame$`Credit Card` = ifelse(M2_final_frame$`HH Durable Key` %in% credit_cards$`HH Durable Key`, 1, 0)

M2_final_frame$`Credit Card` = ifelse(M2_final_frame$`HH Durable Key` %in% credit_cards$`HH Durable Key`,
                                      ifelse(credit_cards$`Account Open Date` >= credit_cards$AccountOpenDate, 1, 0), 0)
#Create Credit  Yes/ No
M2_final_frame$`Cash Card` = ifelse(M2_final_frame$`HH Durable Key` %in% cash_cards$`HH Durable Key`, 1, 0)

#Create DD  Yes/ No
M2_final_frame$`Direct Deposit` = ifelse(M2_final_frame$`HH Durable Key` %in% Direct_Deposit$`HH Durable Key`, 1, 0)

#Create DD  Yes/ No
M2_final_frame$`Bill Pay` = ifelse(M2_final_frame$`HH Durable Key` %in% Bill_Pay$`HH Durable Key`, 1, 0)

#Create DD  Yes/ No
M2_final_frame$`Mobile` = ifelse(M2_final_frame$`HH Durable Key` %in% Mobile_Deposit$`HH Durable Key`, 1, 0)

#Create DD  Yes/ No
M2_final_frame$`Safe Box` = ifelse(M2_final_frame$`HH Durable Key` %in% Safe_Box$`HH Durable Key`, 1, 0)

M2_final_frame$`Total Services` = rowSums(M2_final_frame[,15:30])

M2_final_frame$`Total CON Services` = rowSums(M2_final_frame[,15:20])
M2_final_frame$Drop = ifelse(M2_final_frame$AccountClosed == "Closed",
                             ifelse(M2_final_frame$`Total CON Services` == 0, "drop", "keep"), "keep")

M2_final_frame = filter(M2_final_frame, Drop == "keep")
M2_final_frame = M2_final_frame[,-c(32, 33)]
#M2 Output Writing ##########################################################################################

#Write Full Report History Table to Sandbox
dbWriteTable(dbicon, name = "NRD_HST", M2_final_frame, row.names = FALSE, append=TRUE)

# MONTH 1 Population Creation##########################################################################################

# Here we are using mostly the same SQL queries from earlier but changing the %s variables accordingly.
numb_svcs_M1_string = sprintf(numb_svcs, M1_full_date_string)
M1_starting_df = DBI::dbGetQuery(dbicon, numb_svcs_M1_string)
qual_dda_sql_string = sprintf(qual_dda_sql, M1_full_date_string, M1_first_day_string, M1_last_day_string, M1_last_day_string, M1_first_day_string, M1_last_day_string)
qual_dda = DBI::dbGetQuery(dbicon, qual_dda_sql_string)
qual_dda$AccountClosed = ifelse((is.na(qual_dda$ClosedDate)| qual_dda$ClosedDate > M1_last_day), "Open", "Closed")
qual_dda$DaysOpen = ifelse(qual_dda$AccountClosed == "Closed", round(difftime(qual_dda$ClosedDate , qual_dda$AccountOpenDate, units = c("days")), digits = 0), 90)

#Apply fulldate string to SQL statement
exclude_sql_string = sprintf(exclude_sql, M2_full_date_string, M2_first_day_string, M1_last_day_string)

#Starter pop is HHs that opened a qualifying checking account in M3
exclude = DBI::dbGetQuery(dbicon, exclude_sql_string)

#Apply fulldate string to SQL statement
exclude2_sql_string = sprintf(exclude2_sql, M1_full_date_string, M2_last_day_string)

#Starter pop is HHs that opened a qualifying checking account in M3
exclude2 = DBI::dbGetQuery(dbicon, exclude2_sql_string)

exclude1_hhs = c(unique(exclude$`HH Durable Key`))
exclude1_hhs_string = paste("'",as.character(exclude1_hhs),"'",collapse=", ",sep="")

exclude2_hhs = c(unique(exclude2$`HH Durable Key`))
exclude2_hhs_string = paste("'",as.character(exclude2_hhs),"'",collapse=", ",sep="")

inelig_hhs = c(exclude2_hhs, exclude1_hhs)

qual_hhs = c(unique(qual_dda$`HH Durable Key`))
elig_hhs = qual_hhs[which(!qual_hhs %in% inelig_hhs)]
qual_dda_no_prior = filter(qual_dda,  `HH Durable Key` %in% elig_hhs)

exclude1_cust = c(unique(exclude$`EDW Customer ID`))
exclude1_cust_string = paste("'",as.character(exclude1_cust),"'",collapse=", ",sep="")
exclude2_cust = c(unique(exclude2$`EDW Customer ID`))
exclude2_cust_string = paste("'",as.character(exclude2_cust),"'",collapse=", ",sep="")
inelig_cust = c(exclude1_cust, exclude2_cust)

qual_cust = c(unique(qual_dda_no_prior$`EDW Customer ID`))
elig_cust = qual_cust[which(!qual_cust %in% inelig_cust)]
qual_dda_no_prior = filter(qual_dda_no_prior,  `EDW Customer ID` %in% elig_cust)

qual_dda_no_prior = qual_dda_no_prior %>%  group_by(`HH Durable Key`) %>%filter(row_number() < 2)
qual_dda_no_prior = inner_join(M1_starting_df, qual_dda_no_prior, by=c("HH Durable Key"))
qual_hhs = c(unique(qual_dda_no_prior$`HH Durable Key`))
qual_hhs_string = paste("'",as.character(qual_hhs),"'",collapse=", ",sep="")
qual_acts = c(unique(qual_dda_no_prior$`AccountNumber`))
qual_acts_string = paste("'",as.character(qual_acts),"'",collapse=", ",sep="")
qual_custs = c(unique(qual_dda_no_prior$`EDW Customer ID`))
qual_custs_string = paste("'",as.character(qual_custs),"'",collapse=", ",sep="")

trustee_sql_string = sprintf(trustee_sql, qual_hhs_string, M1_full_date_string, qual_acts_string)
prime_trust = DBI::dbGetQuery(dbicon, trustee_sql_string)
prime_trust = filter(prime_trust, `Account Relationship Type Desc` %in% c("INDIVIDUAL", "TRUST", "TRUSTEE"))
primetrust_sum = prime_trust %>% group_by(`HH Durable Key`, `Account Number`) %>% 
  summarize(type = paste(sort(unique(`Account Relationship Type Desc`)),collapse=", "))
primetrust_combo = filter(primetrust_sum, `type` %in% c("INDIVIDUAL, TRUST, TRUSTEE", "INDIVIDUAL, TRUST", "INDIVIDUAL, TRUSTEE", "TRUST, TRUSTEE"))
prime_trust_hhs = c(unique(primetrust_combo$`HH Durable Key`))
prime_trust_string = paste("'",as.character(prime_trust_hhs),"'",collapse=", ",sep="")

dda_closed_month_prior_sql_string = sprintf(dda_closed_month_prior_sql, M3_full_date_string, qual_hhs_string)
dda_closed = DBI::dbGetQuery(dbicon, dda_closed_month_prior_sql_string)
dda_closed_month_prior = c(unique(dda_closed$`HH Durable Key`))
dda_closed_month_prior_string = paste("'",as.character(dda_closed_month_prior),"'",collapse=", ",sep="")
dda_closed_month_prior_sql_string2 = sprintf(dda_closed_month_prior_sql2, M2_full_date_string, dda_closed_month_prior_string)
dda_closed2 = DBI::dbGetQuery(dbicon, dda_closed_month_prior_sql_string2)
dda_closed_month_prior2 = c(unique(dda_closed2$`HH Durable Key`))
prior_dda_sql_string = sprintf(prior_dda_sql, M2_full_date_string)
prior_dda = DBI::dbGetQuery(dbicon, prior_dda_sql_string)

#Combine ineligible HH Lists. List will contain HHs that closed their only DDA the month prior, have an active DDA, 
#or were erroneous inclusions due to the trust/trustee relationship duplication 
pdda_hhs = c(unique(prior_dda$`HH Durable Key`))
inelig_hhs = c(pdda_hhs, prime_trust_hhs, dda_closed_month_prior2)#, inelig_hhs)#, dda_closed_month_prior2 ) # 
inelig_hhs_string = paste("'",as.character(inelig_hhs),"'",collapse=", ",sep="")

#Now take the ineligible list and subtract it from our qualified HHs to get the true pop.
elig_hhs = qual_hhs[which(!qual_hhs %in% inelig_hhs)]
hh_acct_open = filter(qual_dda_no_prior,  `HH Durable Key` %in% elig_hhs)

#Break out for later use and sub table writing.
hh_acct_open = hh_acct_open[c("HH Durable Key", "AccountOpenDate", "ClosedDate", "AccountNumber", "AverageLedgerBalanceMTD",
                              "EDW Customer ID", "New HH This Month", "AccountClosed", "DaysOpen", "Associate ID", "Store Number")]
hh_acct_open$`Report Month` = paste0(months(M1_full_date, abbr = FALSE), " Month 1")


hh_acct_list = c(unique(hh_acct_open$AccountNumber))
hh_acct_list_string = paste("'",as.character(hh_acct_list),"'",collapse=", ",sep="")
hh_cust_list = c(unique(hh_acct_open$`EDW Customer ID`))
hh_cust_list_string = paste("'",as.character(hh_cust_list),"'",collapse=", ",sep="")

just_hh_durable_keys = hh_acct_open["HH Durable Key"] #changed here 9.3.19
just_hh_durable_keys = unique(just_hh_durable_keys)
just_hh_durable_keys_list =  c(unique(just_hh_durable_keys$`HH Durable Key`))
just_hh_durable_keys_list_string = paste("'",as.character(just_hh_durable_keys_list),"'",collapse=", ",sep="")


#Validate Inital Population ####
#Code to validate population. Keep commented out unless needed.

#
# pop_val = read.delim("Y:/Zack/number_of_services_validation/New folder/pop_val_Nov.txt")
# pop_val = pop_val["H.HH.Durable.Key"]
# pop_val$H.HH.Durable.Key = as.numeric(pop_val$H.HH.Durable.Key)
# 
# false_negatives = anti_join(pop_val, just_hh_durable_keys, by=c("H.HH.Durable.Key" = "HH Durable Key"))
# print(nrow(false_negatives))
# false_negatives_may = inner_join(M3_starting_df, false_negatives, by=c("HH Durable Key" = "H.HH.Durable.Key"))
# #test = read.csv("Y:/Zack/og_false_pos.csv")
# #test = anti_join(false_negatives_may, durable_keys_to_remove, by=c("HH Durable Key"))
# 
# false_positives = anti_join(just_hh_durable_keys, pop_val, by=c("HH Durable Key" = "H.HH.Durable.Key"))
# print(nrow(false_positives))
# false_positives_may = inner_join(M3_starting_df, false_positives, by=c("HH Durable Key" = "HH Durable Key"))
# false_positives_may = inner_join(false_positives_may, qual_dda, by=c("HH Durable Key"))

# M1 Final Frame ##########################################################################################
# M1 CD Products##########################################################################################

#Apply fulldate string to SQL statement
cd_products_sql_string = sprintf(cd_products_sql, M1_full_date_string, M1_first_day_string, M1_last_day_string, M1_last_day_string)

#Starter pop is HHs that opened a qualifying checking account in M3
cd_products = DBI::dbGetQuery(dbicon, cd_products_sql_string)
cd_products = inner_join(cd_products, hh_acct_open, by=c("Customer_ID" = "EDW Customer ID"))

# M1 Savings Products##########################################################################################

saving_products_sql_string = sprintf(savings_products_sql, M1_full_date_string, M1_first_day_string, M1_last_day_string, M1_last_day_string)
saving_products = DBI::dbGetQuery(dbicon, saving_products_sql_string)
saving_products = inner_join(saving_products, hh_acct_open, by=c("Customer_ID" = "EDW Customer ID"))

# M1 MM Products##########################################################################################

mm_products_sql_string = sprintf(mm_products_sql, M1_full_date_string, M1_first_day_string, M1_last_day_string, M1_last_day_string)
mm_products = DBI::dbGetQuery(dbicon, mm_products_sql_string)
mm_products = inner_join(mm_products, hh_acct_open, by=c("Customer_ID" = "EDW Customer ID"))
mm_products = unique(mm_products)
# M1 Loan Products##########################################################################################

loan_sql_string = sprintf(loan_sql, M1_full_date_string, M1_first_day_string, M1_last_day_string, M1_first_day_string, M1_last_day_string, M1_last_day_string)
loans = DBI::dbGetQuery(dbicon, loan_sql_string)
loans = inner_join(loans, hh_acct_open, by=c("Customer_ID" = "EDW Customer ID"))
loans = unique(loans)
# M1 Mortage Products##########################################################################################

mort_sql_string = sprintf(mort_sql, M1_full_date_string, M1_first_day_string, M1_last_day_string, M1_last_day_string)
mortgage = DBI::dbGetQuery(dbicon, mort_sql_string)
mortgage = inner_join(mortgage, hh_acct_open, by=c("Customer_ID" = "EDW Customer ID"))
mortgage = unique(mortgage)

# M1 Digital and Debit ##########################################################################################

q2_products_sql_string = sprintf(q2_sql, M1_full_date_string, hh_acct_list_string)#, M3_last_day_string)
q2_products = DBI::dbGetQuery(dbicon, q2_products_sql_string)
q2_products$CreateDate = ymd_hms(q2_products$CreateDate)
q2_products = filter(q2_products, CreateDate >= M1_first_day)

#Product Break Out
Bill_Pay = filter(q2_products, Service_ID == 7)
Debit_Card = filter(q2_products, Service_ID == 21)
Direct_Deposit = filter(q2_products, Service_ID == 22)
Mobile_Deposit = filter(q2_products, Service_ID == 51)
Safe_Box = filter(q2_products, Service_ID == 38)
#Online_Banking = filter(q2_products, Service_ID == 49)
#GoTo = filter(q2_products, Service_ID == 50)
Online_Statement = filter(q2_products, Service_ID == 56)

go_olb_sql_string = sprintf(go_olb_sql, M1_full_date_string, M1_first_day_string, M1_last_day_string)
go_olb_products = DBI::dbGetQuery(dbicon, go_olb_sql_string)

Online_Banking = filter(go_olb_products, `Acct Prod Product ID` == "QDCX")
GoTo = filter(go_olb_products, `Acct Prod Product ID` == "GOTO")

#Find Credit Cards##########################################################################################

credit_cards_string = sprintf(cc_sql, M1_first_day_string, M1_last_day_string)
credit_cards = DBI::dbGetQuery(dbicon, credit_cards_string)
credit_cards = inner_join(credit_cards, hh_acct_open, by=c("HH Durable Key"))
credit_cards = unique(credit_cards)


#Find Cash Cards##########################################################################################

cash_card_sql_string = sprintf(cash_card_sql, M1_first_day_string, M1_last_day_string)#, M3_last_day_string)
cash_cards = DBI::dbGetQuery(dbicon, cash_card_sql_string)
cash_cards = inner_join(cash_cards, hh_acct_open, by=c("PrimaryCustomer_ID" = "EDW Customer ID"))
cash_cards = unique(cash_cards)

#Create Fields for Final M1 DataFrame##########################################################################################
M1_final_frame = just_hh_durable_keys["HH Durable Key"]
M1_final_frame = inner_join(M1_final_frame, M1_starting_df, by=c("HH Durable Key"))
M1_final_frame = M1_final_frame[-c(3)]
M1_final_frame = inner_join(M1_final_frame, hh_acct_open, by=c("HH Durable Key"))
#M1_final_frame = filter(M1_final_frame, Region != "Digital and Central Banking")

# Remove Dupes
M1_final_frame =M1_final_frame %>%  group_by(`HH Durable Key`) %>%filter(row_number() < 2)

#Create DDA Yes/No
M1_final_frame$DDA = ifelse(is.na(M1_final_frame$`ClosedDate`) | (M1_final_frame$`ClosedDate` > M1_last_day),1,0)
#Create CD Yes/ No
M1_final_frame$CDs = ifelse(M1_final_frame$`HH Durable Key` %in% cd_products$`HH Durable Key`,
                            ifelse(cd_products$AccountOpenDate.x >= cd_products$AccountOpenDate.y, 1, 0), 0)
#Create Savings Yes/ No
M1_final_frame$Savings = ifelse(M1_final_frame$`HH Durable Key` %in% saving_products$`HH Durable Key`,
                                ifelse(saving_products$AccountOpenDate.x >= saving_products$AccountOpenDate.y, 1, 0), 0)

#Create MM Yes/ No
M1_final_frame$MM = ifelse(M1_final_frame$`HH Durable Key` %in% mm_products$`HH Durable Key`,
                           ifelse(mm_products$AccountOpenDate.x >= mm_products$AccountOpenDate.y, 1, 0), 0)                            

#Create Loan Yes/ No
M1_final_frame$`Loan` = ifelse(M1_final_frame$`HH Durable Key` %in% loans$`HH Durable Key`,
                               ifelse(loans$AccountOpenDate.x >= loans$AccountOpenDate.y, 1, 0), 0)

#Create Mortgage Yes/ No
M1_final_frame$`Mortgage` = ifelse(M1_final_frame$`HH Durable Key` %in% mortgage$`HH Durable Key`,
                                   ifelse(mortgage$AccountOpenDate.x >= mortgage$AccountOpenDate.y, 1, 0), 0)

#Create Online Banking Yes/ No
M1_final_frame$`Online Banking` = ifelse(M1_final_frame$`HH Durable Key` %in% Online_Banking$`HH Durable Key`, 1, 0)

#Create GOTO Yes/ No
M1_final_frame$GOTO = ifelse(M1_final_frame$`HH Durable Key` %in% GoTo$`HH Durable Key`, 1, 0)

#Create E Statment  Yes/ No
M1_final_frame$`Online Statements` = ifelse(M1_final_frame$`HH Durable Key` %in% Online_Statement$`HH Durable Key`, 1, 0)

#Create Debit  Yes/ No
M1_final_frame$`Debit Card` = ifelse(M1_final_frame$`HH Durable Key` %in% Debit_Card$`HH Durable Key`, 1, 0)

#Create Credit  Yes/ No
#M1_final_frame$`Credit Card` = ifelse(M1_final_frame$`HH Durable Key` %in% credit_cards$`HH Durable Key`, 1, 0)

M1_final_frame$`Credit Card` = ifelse(M1_final_frame$`HH Durable Key` %in% credit_cards$`HH Durable Key`,
                                      ifelse(credit_cards$`Account Open Date` >= credit_cards$AccountOpenDate, 1, 0), 0)
#Create Credit  Yes/ No
M1_final_frame$`Cash Card` = ifelse(M1_final_frame$`HH Durable Key` %in% cash_cards$`HH Durable Key`, 1, 0)

#Create DD  Yes/ No
M1_final_frame$`Direct Deposit` = ifelse(M1_final_frame$`HH Durable Key` %in% Direct_Deposit$`HH Durable Key`, 1, 0)

#Create DD  Yes/ No
M1_final_frame$`Bill Pay` = ifelse(M1_final_frame$`HH Durable Key` %in% Bill_Pay$`HH Durable Key`, 1, 0)

#Create DD  Yes/ No
M1_final_frame$`Mobile` = ifelse(M1_final_frame$`HH Durable Key` %in% Mobile_Deposit$`HH Durable Key`, 1, 0)

#Create DD  Yes/ No
M1_final_frame$`Safe Box` = ifelse(M1_final_frame$`HH Durable Key` %in% Safe_Box$`HH Durable Key`, 1, 0)

M1_final_frame$`Total Services` = rowSums(M1_final_frame[,15:30])

M1_final_frame$`Total CON Services` = rowSums(M1_final_frame[,15:20])
M1_final_frame$Drop = ifelse(M1_final_frame$AccountClosed == "Closed",
                             ifelse(M1_final_frame$`Total CON Services` == 0, "drop", "keep"), "keep")

M1_final_frame = filter(M1_final_frame, Drop == "keep")
M1_final_frame = M1_final_frame[,-c(32, 33)]

#M1 Output Writing ##########################################################################################

#Write Full Report History Table to Sandbox
dbWriteTable(dbicon, name = "NRD_HST", M1_final_frame, row.names = FALSE, append=TRUE)
print(paste0("Finished at: ", Sys.time()))
