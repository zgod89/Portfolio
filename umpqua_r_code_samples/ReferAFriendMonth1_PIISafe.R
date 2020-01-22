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


# Find all customers with an ACH In Transaction greater than 500 since 2019-10-1
trans = "
select distinct
a.[AccountNumber] as 'Account Number'
FROM [EDWLoansandDeposits].[dbo].[Accounts] a
inner join [AccountTransactions].[dbo].[AccountTransactions] t
on a.Account_ID = t.Account_ID
and t.[ACHInAmount] >= 500
and t.PostingDate <= dateadd(day,90,a.AccountOpenDate)
where t.PostingDate > '2019-10-1'
"

trans_df = DBI::dbGetQuery(dbitran, trans)

# Find eligible reffered friends and corresponding referrers
cust_sql = "
DECLARE @today date = getdate()

select distinct
pr.ProductName as 'Product Name'
,a.[AccountNumber] as 'Account Number'
,cc.CostCenter as 'RF Cost Center'
,AVG(d.[AverageLedgerBalanceMTD]) as 'AverageLedgerBalanceMTD'
,min(a.[AccountOpenDate]) as 'Date Opened'
,c.CustomerSinceDate 'Customer Since Date'
,c.[CustomerName] as 'Customer Name'
,a.[PrimaryCustomer_ID] as 'EDW Customer ID'
,g.[Referral_Code] as 'Referral Code'
,g.[CustomerName] as 'Referrer'
,g.[IBSCustomerNumber] as 'Referrer ID'
,a2.[AccountNumber] as 'Refferer Account Number'
,cc2.CostCenter as 'Referrer Cost Center'


FROM [EDWLoansandDeposits].[dbo].[Accounts] a
inner join [EDWLoansandDeposits].[dbo].[AccountPromotions] ac
on a.Account_ID = ac.Account_ID

left join [EDWReportingDatamart].[dbo].[vw_dimServicingCostCenter] cc
on a.[ServicingCostCenter_ID] = cc.CostCenter_ID
and cc.IsCurrentYN = 1

inner join [EDWLoansandDeposits].[dbo].[DepositsMonthly] d
on a.Account_ID = d.Account_ID

inner join [EDW_Sandbox].[dbo].[Grow_Seattle_refer_a_friend_eligible_Cust_201909] g
on g.[Referral_Code] = ac.[AccountPromotionCode]
and ac.Promotion_ID = 1

inner join [EDWLoansandDeposits].[dbo].[Customers] c
on a.[PrimaryCustomer_ID] = c.[Customer_ID]
and dateadd(day, 6570, c.[BirthDate]) <= dateadd(day,90,a.AccountOpenDate)

inner join [EDWLoansandDeposits].[dbo].[Products] pr
on a.Product_ID = pr.Product_ID
and pr.Product_ID in ('178', '230', '250', '283')

inner join [EDWLoansandDeposits].[dbo].[Customers] c2
on g.[IBSCustomerNumber] = c2.[IBSCustomerNumber]
and dateadd(day, 6570, c.[BirthDate]) <= dateadd(day,90,a.AccountOpenDate)

inner join [EDWLoansandDeposits].[dbo].[Accounts] a2
on a2.[PrimaryCustomer_ID] = c2.[Customer_ID]
and a2.Product_ID in ('178',	'179',	'183',	'185',	'192',	'199',	'204',	'205',	'207',	'208',	'217',	'220',	'230',	'233',	'234',	'237',	'245',	'247',	'250',	'257',	'260',	'283')

left join [EDWReportingDatamart].[dbo].[vw_dimServicingCostCenter] cc2
on a2.[ServicingCostCenter_ID] = cc2.CostCenter_ID
and cc2.IsCurrentYN = 1

where (a.AccountOpenDate between '2019-10-7' AND '2019-12-31')
and datediff(dd, a.AccountOpenDate, @today) >=90
and (a.[ClosedDate] > dateadd(day,90,a.AccountOpenDate) OR a.ClosedDate is null)

group by 
pr.ProductName 
,a.[AccountNumber] 
,c.CustomerSinceDate 
,c.[CustomerName] 
,a.[PrimaryCustomer_ID] 
,g.[Referral_Code] 
,g.[CustomerName] 
,g.[IBSCustomerNumber] 
,cc.CostCenter
,a2.[AccountNumber] 
,cc2.CostCenter 
"

cust_df = DBI::dbGetQuery(dbicon, cust_sql)

# Generate Payouts for Referred Friends ####
referred_friends_to_pay = inner_join(cust_df, trans_df, by= c("Account Number"))
# Remove Dupes
referred_friends_to_pay = referred_friends_to_pay %>%  group_by(`EDW Customer ID`) %>%filter(row_number() < 2)
#Split DataFrame
referrers_to_pay = referred_friends_to_pay[, c(9:13)]
referred_friends_to_pay = referred_friends_to_pay[, c(1:8)]
# Check for prexisting personal checking account 
rf_ids = c(unique(referred_friends_to_pay$`EDW Customer ID`))
rf_ids_string = paste("'",as.character(rf_ids),"'",collapse=", ",sep="")
rf_aod = c(unique(referred_friends_to_pay$`Date Opened`))
rf_aod_string = paste("'",as.character(rf_aod),"'",collapse=", ",sep="")

prex_check_sql = "
DECLARE @today date = getdate()

select distinct
a.[PrimaryCustomer_ID] as 'EDW Customer ID'
,a.[AccountNumber] as 'Account Number'
,min(a.[AccountOpenDate]) as 'Date Opened'
FROM [EDWLoansandDeposits].[dbo].[Accounts] a
inner join [EDWLoansandDeposits].[dbo].[Customers] c
on a.[PrimaryCustomer_ID] = c.[Customer_ID]
inner join [EDWLoansandDeposits].[dbo].[Products] pr
on a.Product_ID = pr.Product_ID
where a.AccountOpenDate between dateadd(day,-182, @today) and @today -- *checks to see if checking accounts were opened on any day other than the day of the qualifying acct within 6 months of today 
and a.[PrimaryCustomer_ID] in (%s)
and a.[AccountOpenDate] not in (%s)
and pr.ProductCategory_ID in ('5','6','7', '11', '14', '12')
group by 
a.[AccountNumber] 
,a.[PrimaryCustomer_ID] 
"
prex_check_sql_str = sprintf(prex_check_sql, rf_ids_string, rf_aod_string)
prex_check = DBI::dbGetQuery(dbicon, prex_check_sql_str)
# Remove Ineligble Customers Based on Previous Relationship

referred_friends_to_pay = anti_join(referred_friends_to_pay, prex_check, by=c("EDW Customer ID"))

# Add date and payment due variables
referred_friends_to_pay$`Amount Due` = "100"
referred_friends_to_pay$`AsOfDate` = today()


#Write Referred Friend History Table to Sandbox
dbWriteTable(dbicon, name = "Grow_Seattle_Referred_Friend_Payment_HST", referred_friends_to_pay, row.names = FALSE, append=TRUE)

# Generate Payouts for Referrers ####
# Summarize
referrers_to_pay_sum = referrers_to_pay %>%  group_by(`Referral Code`, `Referrer`,`Referrer ID`, `Refferer Account Number`, `Referrer Cost Center`) %>% summarise( `Referred Friends This Month` = n())
referrers_to_pay_sum$`Sum RF To Date` = referrers_to_pay_sum$`Referred Friends This Month`
referrers_to_pay_sum$`Amount Due` = referrers_to_pay_sum$`Referred Friends This Month` * 50
referrers_to_pay_sum$`AsOfDate` = today()

#Write Referred Friend History Table to Sandbox
dbWriteTable(dbicon, name = "Grow_Seattle_Referrers_Payment_HST", referrers_to_pay_sum, row.names = FALSE, append=TRUE)

