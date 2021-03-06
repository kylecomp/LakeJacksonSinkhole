#Clean Well Data

#Path to Wells Folder
source(file.path("code", "paths+packages.R"))
raw_data_path<-file.path("data", "raw", "wells")

#create List of all file Names 
file_list <- list.files(path=raw_data_path, pattern="*.csv")

# now read all files to list:
mycsv = dir(pattern=".csv")
n <- length(file_list)
mylist <- vector("list", n)
for(i in 1:n){mylist[[i]] <- read.csv(paste(raw_data_path,'/', file_list[i], sep = ''), skip=14)}

# Clean all of the Dataframes to only have a Timestamp and Value (GW Level)
mylist <- lapply(mylist, function(x) {
  x[,c(1, 4,5,6)]<-list(NULL) 
  names(x)[1]<-"Datetime"
  names(x)[2]<-"GW_Value"
  return(x)})

##Apply Name difference for each GW_Value
for (i in 1:n){
  names(mylist[[i]])[[2]]<-as.character(file_list[i])
}

##Create new dataframe and combine all dataframes into it
df_WellCompilation<-as.data.frame(mylist[1])
for (i in 2:n){
  temp<-as.data.frame(mylist[i])
  df_WellCompilation<-left_join(df_WellCompilation, temp)
  temp<-NULL
}

#Write to CSV File
readr::write_csv(df_WellCompilation, file.path("data", "processed", "CombinedWellData.CSV"))
