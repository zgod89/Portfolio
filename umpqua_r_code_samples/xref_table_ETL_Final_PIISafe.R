library(readxl)
library(dplyr)
library(openxlsx)
library(getPass)
library(odbc)
library(DBI)
library(RODBC)
library(lubridate)


options(scipen=999) #gets rid of scientific notation


# Write number of services file to Sandbox

dbicon <- DBI::dbConnect(odbc::odbc(),
                         
                         driver = "SQL Server",
                         
                         server =  "",
                         
                         database = "EDW_Sandbox"
                         
)


file_path = "C:\\Users\\ZackGodwin\\Downloads\\Relationship Export Files2\\"

# Read files and manipulate into dataframe ####
file_list = list.files(path=file_path, pattern="*.xlsx", full.names=TRUE, recursive=FALSE)

#Create Log File
log_con = file("C:\\Users\\ZackGodwin\\Downloads\\Relationship Export Files\\log.txt", open="a")

full_date_sql =  "with sub as(
select 
distinct([Full Date])
FROM [EDW_Sandbox].[dbo].[Number_Of_Services_HST])  

select top 1 * from sub --normally should be 5, changed to 6 for testing
ORDER BY sub.[Full Date] DESC
"
dates = DBI::dbGetQuery(dbicon, full_date_sql)
date_list =  c(unique(dates$`Full Date`))
full_date = date_list[1]
full_date_string=toString(sprintf("'%s'", full_date)) 


for (i in file_list){
  i = file_list[1]
  path = i
  
  cat(paste0("Beginning to Process ", i,  " at: ", Sys.time(), "on ", Sys.Date()), file = log_con, sep="\n")
  
  #Read in the xlsx workbook, which only has column names on tab 1. Each workbook has several tabs of data.
  sheets <- openxlsx::getSheetNames(path)
  #Create a list of dataframes, where each sheet is a unique dataframe.
  SheetList <- lapply(sheets,openxlsx::read.xlsx, xlsxFile=path, startRow = 4, colNames = TRUE)
  #Name each dataframe as names in the workbook tab.
  names(SheetList) <- sheets
  
  #State desired column names, since only sheet 1 has column headers we need to enforce for the rest.
  colnames = c("Account Durable Key", "Account Number", "Account Open Date", "Acct Prod Product ID", "Acct Prod Product Desc", "Open Closed Acct Code", "CIS Customer Number", "Customer Durable Key",
               "HH Durable Key", "Account Relationship Type Desc",
               "Primary Customer Code", "Full Date")
  #Apply column names to each dataframe in my list of dataframes.
  SheetList = lapply(SheetList, setNames, colnames)
  #Combine each dataframe in my list of dataframes into a single dataframe/
  library(plyr)
  master_table = rbind.fill(SheetList)
  detach("package:plyr", unload=TRUE) 
  #Convert Full Date from excel date code to a readable date.
  master_table$`Full Date` = convertToDate(master_table$`Full Date`)
  master_table$`Account Open Date` = convertToDate(master_table$`Account Open Date`)
  master_table$`CIS Customer Number` = as.numeric(master_table$`CIS Customer Number`)
  
  hh_sql = "
  select [C_Customer_Number]
	,[Customer_ID] as 'EDW Customer ID'
	,[H_CIS_Household_Number]
	from [dbo].[DB_EXTRACTS_JUL19_DEC19] 
	where [Full Date] = %s
  "
  hh_sql_str = sprintf(hh_sql, full_date_string)
  
  hh_table =  DBI::dbGetQuery(dbicon, hh_sql_str)
  
  update_table = left_join(master_table, hh_table, by=c("CIS Customer Number" = "C_Customer_Number"))
  
  update_table = update_table[c(1:8, 13:14, 9:12)]
  
  
  
  dbWriteTable(dbicon, name = "Customer_Analytics_XRef_TEST", update_table, row.names = FALSE, append=TRUE)
 
}

close(log_con)
