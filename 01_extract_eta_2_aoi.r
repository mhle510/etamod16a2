library(tidyverse)
library(rgdal)
library(raster)
library(parallel)
library(rgeos)
library(lubridate)
library(tictoc)
##### FUNCTION

single_calc_areal_value_weights = function(aoiShp, rawRas){
  
  #rawRas = croppedRas 
  t = data.frame(raster::extract(rawRas, aoiShp, weights = TRUE, normalizeWeights= TRUE))
  t = t[complete.cases(t),]
  
  if(sum(t[,2]) != 1){ # check weight is equal to 1
    t[,2] = t[,2]*1/(sum(t[,2]))
    arealVal = sum(t[,1]*t[,2])  
  } else {
    arealVal = sum(t[,1]*t[,2])  
  }
  t
  return(arealVal)
}
################ EXTRACT MODIS ET TO AREA OF INTEREST ##############
mainPath = 'D:/0.NCKH/0.MyPaper/2022_ET_SWAT'
setwd(mainPath)

tifPath = 'E:/GlobalET/MOD16A2_V6_vn/rawtif'
shpPath = file.path(mainPath, 'catchment_shape_files')

dir.create(file.path(mainPath,'processed'), showWarnings = F)
dir.create(file.path(mainPath,'processed','inter_csv'), showWarnings = F)
processedPath = file.path(mainPath,'processed','inter_csv')
# get catchment name
catchNames = list.files(shpPath, pattern = '.shp') %>% str_remove_all('.shp')
nc = length(catchNames)
# read rasfiles
no_cores = detectCores(logical = TRUE)
cl = makeCluster(13)  


listrasFiles = list.files(tifPath, pattern = 'tif', recursive = T, full.names = T)
nr = length(listrasFiles)
doy = substr(basename(listrasFiles), 24, 30)
originDate = paste(substr(doy,1,4),'-01-01', sep = '')
date = as.Date((as.numeric(substr(doy,5,7))-1), origin = originDate)
date0 = paste0(formatC(year(date), width = 4, flag = 0),
               formatC(month(date), width = 2, flag = 0),
               formatC(day(date), width = 2, flag = 0), sep = '')


for(ii in 2:nc){
  cat('------catchment name:', catchNames[ii],'------','\n')
  aoi = readOGR(paste0(shpPath, '/', catchNames[ii],'.shp'), verbose = F)
  CRS.wgs = CRS("+init=epsg:4326")
  
  # convert and union all subbasins 
  aoiwgs = spTransform(aoi, CRS.wgs)
  aoiwgsSingle = aoiwgs
  aoiwgs = geometry(aoiwgs)
  # assign 1 for polygonID
  aoiwgsSingle$PolygonId = 1
  # dissovle automatically basinShpwgsSingle
  rgeos::set_RGEOS_CheckValidity(2L)
  aoiwgsSingle = gUnaryUnion(aoiwgsSingle, id = aoiwgsSingle$PolygonId)
  #plot(aoiwgsSingle)
  
  clusterExport(cl, 'aoiwgsSingle')
  clusterEvalQ(cl, library("raster"))
  
  tic()
  t = parLapply(cl = cl, 1:nr,
                function(ll,listrasFiles, single_calc_areal_value_weights){
                  rawRas = raster(listrasFiles[[ll]])
                  # assigned NA
                  rawRas[rawRas> 3270] = NA
                  rawRas[rawRas < 0] = NA
                  rawRas = rawRas*0.1 #convert unit 0.1mm/8 days to mm/8days
                  single_calc_areal_value_weights(aoiwgsSingle, rawRas)
                }, listrasFiles, single_calc_areal_value_weights# you need to pass these variables into the clusters
  )
  toc()
  
  et = data.frame(dates = date0,
                  et_mm8 = unlist(t)) 
  
  et$et_mm8[et$et_mm8 == 0] =NA 
  summary(et$et_mm8)

  opFile = paste0(processedPath,'/',catchNames[ii],'.csv')
  write.csv(et,opFile, row.names = F)
  
}

