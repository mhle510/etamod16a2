library(tidyverse)
library(rgdal)
library(raster)
library(parallel)
library(rgeos)
library(lubridate)
library(tictoc)
################ MONTHLY PROCESSING##############
library(tidyverse)
library(lubridate)

mainPath = 'D:/0.NCKH/0.MyPaper/2022_ET_SWAT'
setwd(mainPath)

dir.create(file.path(mainPath,'processed','final_csv'), showWarnings = F)
processedPath = file.path(mainPath,'processed','final_csv')

csvFiles = list.files(file.path(mainPath, 'processed','inter_csv'), pattern = '.csv', full.names = T)
catchNames = basename(csvFiles) %>% str_remove_all('.csv')
nc = length(catchNames)


monval = as.Date(seq(as.Date("01-01-2001",format = "%d-%m-%Y"),
                     as.Date("01-12-2020",format = "%d-%m-%Y"),by = "month"))
datmF = data.frame(dates = monval,
                   mat.or.vec(length(monval),nc))
colnames(datmF) = c("dates", catchNames)
for(ii in 1:nc){
  dat = read.csv(csvFiles[ii])
  dat$et_mm = dat$et_mm8/8  # average for 8 days
  dat$dates = make_date(year = substr(dat$dates,1,4), month = substr(dat$dates,5,6), day = substr(dat$dates,7,8))
  
  #t  = data.frame(time1 = dat$dates[1:918],
  #                time2 = dat$dates[2:919] )
  #difft = t$time2 - t$time1
  #which(difft != 8)
  
  # disaggregate 8 days to daily
  datd = NULL
  for(jj in 1 : nrow(dat)){
    subdatd = data.frame(dates = seq((dat$dates[jj] - 7), dat$dates[jj],1),
                         etd = dat$et_mm[jj])
    datd = rbind.data.frame(datd, subdatd)
  }
  
  # pocess duplicated values
  datesdatd = unique(datd$dates)
  datdF = NULL
  for(kk in 1 : length(datesdatd)){
    print(datesdatd[kk])
    locid = which(datd$dates %in% datesdatd[kk])
    if(length(locid) == 1){
      datdF = rbind.data.frame(datdF, datd[locid,])
    } else {
      tempdat = data.frame(dates = datesdatd[kk],
                           etd = mean(datd[locid,2], na.rm = T))
      datdF = rbind.data.frame(datdF, tempdat)
    }
  }
  
  datdF2 = data.frame(yy = year(datdF$dates),
                      mm = month(datdF$dates),
                      etd = datdF$etd)
  datm = aggregate(etd ~ yy + mm, data = datdF2, FUN = mean)
  datm =datm[order(datm$yy),]
  datmF[,ii+1] = datm$etd[-1]
}

nd = days_in_month(datmF$dates)
datmF2 = datmF
datmF2[,-1] = datmF[,-1]*nd
datmF2[,-1] = round(datmF2[,-1],3)
write.csv(datmF2, file.path(processedPath,'eta_modis16A2.csv'), row.names = F)
