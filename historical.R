# get the last timestamp in [r] database
# args=c("folder","symbol","startingtime")

readRDS2=function(filename){
        if(file.exists(filename)){
                md=readRDS(filename)
        }else{
                md=data.frame()
        }
        return(md)
}

getLastTimeStamp=function(folder,file){
        md=readRDS2(paste(folder,file,sep=""))
        return(md)
}



getStartingTime=function(folder,symbol,endingtime,subfolder,lookback=100){
        endingtime=as.Date(strptime(endingtime,format="%Y%m%d",tz="Asia/Kolkata"))
        endingtime=as.character(endingtime)
        if(subfolder>0){
                dirs=list.dirs(folder,recursive = FALSE,full.names = FALSE)
                dirs=dirs[dirs<=endingtime]
                dirs=sort(dirs,decreasing = TRUE)
                dirs=dirs[1:lookback]
                for(d in dirs){
                        file=paste(symbol,"_",d,".rds",sep="")
                        out=getLastTimeStamp(paste(folder,d,"/",sep=""),file)
                        if(nrow(out)>0){
                                return(strftime(out[nrow(out),"date"],"%Y-%m-%d %H:%M:%S",tz="Asia/Kolkata"))
                        }
                }

        }else{
                endingtime=as.POSIXct(endingtime,tz="Asia/Kolkata")
                filename=paste(folder,"/",symbol,".rds",sep="")
                md=readRDS2(filename)
                if(nrow(md)>0){
                        md=md[md$date<=endingtime,]
                        if(nrow(md)>0){
                                return(strftime(md[nrow(md),"date"],"%Y-%m-%d %H:%M:%S",tz="Asia/Kolkata"))
                        }
                }
                return("1970-01-01 00:00:00")
        }
        return("1970-01-01 00:00:00")
}


insertIntoRDB=function(file,time,open,high,low,close,volume,symbol){
        md=readRDS2(file)
        incrmd=data.frame(date=as.POSIXct(as.numeric(time),origin="1970-01-01",tz="Asia/Kolkata"),open=open,high=high,low=low,close=close,volume=as.numeric(volume),symbol=symbol,stringsAsFactors = FALSE)
        #incrmd=data.frame(date=time,open=open,high=high,low=low,close=close,volume=volume)
        md=rbind(md,incrmd)
        dirname=dirname(file)
        dirname=gsub("/","\\",dirname)
        if(!dir.exists(dirname(file))){
                dir.create(dirname(file),recursive = TRUE)
        }
        md=md[order(md$date),]
        md=unique(md)
        rownames(md)=NULL
        saveRDS(md,file=file)
        gc()
        # saveRDS(incrmd,file=file)
}
