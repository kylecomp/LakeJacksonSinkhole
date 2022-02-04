source(file.path("code", "paths+packages.R"))
data_path1<-file.path("data", "processed", "CombinedWellData.CSV")

WellData<- readr::read_csv(data_path1, col_types = cols())


Sullivan<-ggplot(data=WellData)+
  geom_point(aes(x=Datetime, y=FGS.Sullivan_Sink.csv))+
  geom_point(aes(x=Datetime, y=NWFWMD.Bike_Trail.csv))

  geom_point(aes(x=Datetime, y=FGS.Sullivan_Sink.csv))+
  geom_point(aes(x=Datetime, y=FGS.Sullivan_Sink.csv))+
  geom_point(aes(x=Datetime, y=FGS.Sullivan_Sink.csv))+
  geom_point(aes(x=Datetime, y=FGS.Sullivan_Sink.csv))+
  geom_point(aes(x=Datetime, y=FGS.Sullivan_Sink.csv))+
  geom_point(aes(x=Datetime, y=FGS.Sullivan_Sink.csv))+
  geom_point(aes(x=Datetime, y=FGS.Sullivan_Sink.csv))+

Sullivan


###Two Wells are read as logic instead of numerics. Fix That