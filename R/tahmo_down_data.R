get.tahmo.api <- function(aws_dir, adt_dir){
    tz <- "Africa/Blantyre"
    tahmo_time <- "%Y-%m-%dT%H:%M:%SZ"
    last_format <- "%Y%m%d%H%M%S"
    Sys.setenv(TZ = tz)

    dirOUT <- file.path(aws_dir, "AWS_DATA", "DATA", "TAHMO")
    if(!dir.exists(dirOUT))
        dir.create(dirOUT, showWarnings = FALSE, recursive = TRUE)
    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "TAHMO")
    dirLOG1 <- file.path(adt_dir, "AWS_DATA", "LOG", "TAHMO")

    mon <- format(Sys.time(), '%Y%m')
    logDOWN <- file.path(dirLOG, paste0("download_tahmo_", mon, ".txt"))

    crdFile <- file.path(aws_dir, "AWS_DATA", "CSV", "tahmo_crds.csv")
    awsCrd <- utils::read.table(crdFile, sep = ',', header = TRUE,
                                stringsAsFactors = FALSE, quote = "\"")
    lastFile <- file.path(aws_dir, "AWS_DATA", "CSV", "tahmo_lastDown.csv")
    awsLast <- utils::read.table(lastFile, sep = ',', header = TRUE, 
                                 colClasses = "character", na.strings = "",
                                 stringsAsFactors = FALSE, quote = "\"")

    api <- tahmo.api(aws_dir)$connection
    dataset <- 'controlled'

    for(j in seq_along(awsCrd$id)){
        if(trimws(awsCrd$data_status[j]) == 'n') next

        endpoint <- paste('services/measurements/v2/stations',
                           awsCrd$tahmo_id[j], 'measurements',
                           dataset, sep = '/')
        api_url <- paste0(api$url, "/", endpoint)

        awsID <- awsCrd$id[j]
        ilst <- which(awsLast$id == awsID)
        if(length(ilst) == 0){
            tmpLast <- data.frame(id = awsID, last = NA)
            awsLast <- rbind(awsLast, tmpLast)
            ilst <- length(awsLast$id)
        }

        if(is.na(awsLast$last[ilst])){
            init_time <- "20180101000000"
            ## last 5 days (comment at 1st run)
            # init_time <- format(Sys.time() - 5 * 24 * 3600, last_format)
            ####
            last <- as.POSIXct(init_time, format = last_format, tz = tz)
        }else{
            last <- as.POSIXct(awsLast$last[ilst], format = last_format, tz = tz) + 1
            ## last 5 days (comment at 1st run)
            # last5d <- Sys.time() - 5 * 24 * 3600
            # if(last < last5d) last <- last5d
        }
        daty <- seq(last, Sys.time(), 'day')
        daty <- time_local2utc_time(daty)
        if(length(daty) == 1){
            daty <- c(daty, time_local2utc_time(Sys.time()))
        }

        for(s in 1:(length(daty) - 1)){
            ss <- if(s == 1) 0 else 1
            start <- format(daty[s] + ss, tahmo_time)
            end <- format(daty[s + 1], tahmo_time)

            qres <- try(httr::GET(api_url, httr::accept_json(),
                        httr::authenticate(api$id, api$secret),
                        httr::timeout(300),
                        config = httr::config(connecttimeout = 120),
                        query = list(start = start, end = end)),
                    silent = TRUE)

            if(inherits(qres, "try-error")){ 
                mserr <- gsub('[\r\n]', '', qres[1])
                msg <- paste("Unable to download data for:", awsID)
                format.out.msg(paste(mserr, '\n', msg), logDOWN)
                fileLog <- file.path(dirLOG1, basename(logDOWN))
                file.copy(logDOWN, fileLog)
                next
            }

            if(httr::status_code(qres) != 200) next

            resc <- httr::content(qres)
            if(is.null(resc$results[[1]]$series)) next
            if(resc$results[[1]]$statement_id != 0) next

            hdr <- unlist(resc$results[[1]]$series[[1]]$columns)
            val <- lapply(resc$results[[1]]$series[[1]]$values, function(x){
                sapply(x, function(v) if(is.null(v)) NA else v)
            })
            val <- do.call(rbind, val)
            val <- as.data.frame(val)
            names(val) <- hdr

            temps <- strptime(val$time, tahmo_time, tz = 'UTC')
            trg <- range(temps)
            awsLast$last[ilst] <- time_utc2local_char(trg[2], last_format, tz)

            trg <- format(trg, last_format)
            trg <- paste0(trg, collapse = '_')
            fileLoc <- paste0(awsCrd$tahmo_id[j], '_', trg, '.csv')
            fileLoc <- file.path(dirOUT, fileLoc)

            utils::write.table(val, fileLoc, sep = ",", na = "", quote = FALSE,
                                col.names = TRUE, row.names = FALSE)
            utils::write.table(awsLast, lastFile, sep = ",", na = "", col.names = TRUE,
                               row.names = FALSE, quote = FALSE)
        }
    }

    return(0)
}
