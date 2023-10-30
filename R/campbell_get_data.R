get.campbell.data <- function(aws_dir, adt_dir){
    on.exit({
        RODBC::odbcCloseAll()
        if(upload) ssh::ssh_disconnect(session)
    })

    tz <- "Africa/Blantyre"
    Sys.setenv(TZ = tz)
    origin <- "1970-01-01"
    init_time <- "20230601000000"
    last_format <- "%Y%m%d%H%M%S"
    awsNET <- 1

    dirOUT <- file.path(aws_dir, "AWS_DATA", "DATA", "CAMPBELL")
    if(!dir.exists(dirOUT))
        dir.create(dirOUT, showWarnings = FALSE, recursive = TRUE)
    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "CAMPBELL")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)

    mon <- format(Sys.time(), '%Y%m')
    logPROC <- file.path(dirLOG, paste0("processing_campbell_", mon, ".txt"))
    awsLOG <- file.path(dirLOG, paste0("AWS_LOG_", mon, ".txt"))
    uploadFailed <- file.path(dirLOG, 'upload_failed.txt')

    conn <- connect.campbell(aws_dir)
    if(is.null(conn)){
        msg <- "An error occurred when connecting to CAMPBELL database"
        format.out.msg(msg, logPROC)
        return(1)
    }

    session <- connect.ssh(aws_dir)
    if(is.null(session)){
        msg <- "Unable to connect to ADT server"
        format.out.msg(msg, logPROC)
        upload <- FALSE
    }else{
        dirUPData <- file.path(adt_dir, "AWS_DATA", "DATA", "minutes", "CAMPBEL")
        dirUPLog <- file.path(adt_dir, "AWS_DATA", "LOG", "CAMPBEL")
        ssh::ssh_exec_wait(session, command = c(
            paste0('if [ ! -d ', dirUPData, ' ] ; then mkdir -p ', dirUPData, ' ; fi'),
            paste0('if [ ! -d ', dirUPLog, ' ] ; then mkdir -p ', dirUPLog, ' ; fi')
        ))
        upload <- TRUE
    }

    varFile <- file.path(aws_dir, "AWS_DATA", "CSV", "campbell_pars.csv")
    varTable <- utils::read.table(varFile, sep = ',', header = TRUE,
                                  stringsAsFactors = FALSE, quote = "\"",
                                  fileEncoding = 'UTF-8')
    crdFile <- file.path(aws_dir, "AWS_DATA", "CSV", "campbell_crds.csv")
    awsCrd <- utils::read.table(crdFile, sep = ',', header = TRUE,
                                stringsAsFactors = FALSE, quote = "\"",
                                fileEncoding = 'UTF-8')
    lastFile <- file.path(aws_dir, "AWS_DATA", "CSV", "campbell_lastDates.csv")
    awsLast <- utils::read.table(lastFile, sep = ',', header = TRUE, 
                                 colClasses = "character", na.strings = "",
                                 stringsAsFactors = FALSE, quote = "\"")

    for(j in seq_along(awsCrd$id)){
        awsID <- awsCrd$id[j]
        awsVAR <- varTable[varTable$id == awsID, , drop = FALSE]

        ilst <- which(awsLast$id == awsID)
        if(length(ilst) == 0){
            tmpLast <- data.frame(id = awsID, last = NA)
            awsLast <- rbind(awsLast, tmpLast)
            ilst <- length(awsLast$id)
        }

        if(is.na(awsLast$last[ilst])){
            last <- as.POSIXct(init_time, format = last_format, tz = tz)
        }else{
            last <- as.POSIXct(awsLast$last[ilst], format = last_format, tz = tz)
        }
        lastCBS <- format(last, "%Y-%m-%d %H:%M:%S.0000000")
        last <- as.numeric(last)

        #### comment 1st run
        query <- paste0("SELECT * FROM [dbo].[", awsCrd$dbTable_name[j], "] WHERE TmStamp > '", lastCBS, "'")
        qres <- try(RODBC::sqlQuery(conn, query), silent = TRUE)

        # #### uncomment 1st run
        # query <- paste0("SELECT * FROM [dbo].[", awsCrd$dbTable_name[j], "]")
        # qres <- try(RODBC::sqlQuery(conn, query), silent = TRUE)
        # ###

        if(inherits(qres, "try-error")){
            msg <- paste("Unable to get data, aws_id:", awsID)
            format.out.msg(paste(msg, '\n', qres), awsLOG)
            next
        }

        if(nrow(qres) == 0) next

        out <- try(parse.campbell_db.data(qres, awsVAR, awsID, awsNET), silent = TRUE)
        if(inherits(out, "try-error")){
            mserr <- gsub('[\r\n]', '', out[1])
            msg <- paste("Unable to parse data for", awsID)
            format.out.msg(paste(mserr, '\n', msg), awsLOG)
            next
        }

        if(is.null(out)) next

        ########### comment 1st run
        it <- out$obs_time > last
        out <- out[it, , drop = FALSE]
        if(nrow(out) == 0) next

        ###########

        temps <- as.POSIXct(out$obs_time, origin = origin, tz = tz)
        split_day <- format(temps, '%Y%m%d')
        index_day <- split(seq(nrow(out)), split_day)

        for(s in seq_along(index_day)){
            don <- out[index_day[[s]], , drop = FALSE]
            lastV <- as.POSIXct(max(don$obs_time), origin = origin, tz = tz)
            awsLast$last[ilst] <- format(lastV, last_format)

            locFile <- paste(range(don$obs_time), collapse = "_")
            locFile <- paste0(awsID, "_", locFile, '.rds')
            locFile <- file.path(dirOUT, locFile)
            saveRDS(don, locFile)

            utils::write.table(awsLast, lastFile, sep = ",", na = "", col.names = TRUE,
                               row.names = FALSE, quote = FALSE)

            if(upload){
                upFile <- file.path(dirUPData, basename(locFile))
                ret <- try(ssh::scp_upload(session, locFile, to = upFile, verbose = FALSE), silent = TRUE)

                if(inherits(ret, "try-error")){
                    if(grepl('disconnected', ret[1])){
                        Sys.sleep(1)
                        session <- connect.ssh(aws_dir)
                        upload <- if(is.null(session)) FALSE else TRUE
                    }
                    cat(basename(locFile), file = uploadFailed, append = TRUE, sep = '\n')
                }
            }else{
                cat(basename(locFile), file = uploadFailed, append = TRUE, sep = '\n')
            }
        }
    }

    if(upload){
        if(file.exists(logPROC)){
            logPROC1 <- file.path(dirUPLog, basename(logPROC))
            ssh::scp_upload(session, logPROC, to = logPROC1, verbose = FALSE)
        }

        if(file.exists(awsLOG)){
            awsLOG1 <- file.path(dirUPLog, basename(awsLOG))
            ssh::scp_upload(session, awsLOG, to = awsLOG1, verbose = FALSE)
        }
    }

    return(0)
}

parse.campbell_db.data <- function(awsDAT, awsVAR, awsID, awsNET){
    tz <- "Africa/Blantyre"
    campbell_time <- "%Y-%m-%d %H:%M:%S"
    Sys.setenv(TZ = tz)

    temps <- strptime(awsDAT$TmStamp, campbell_time, tz)
    ina <- is.na(temps)
    awsDAT <- awsDAT[!ina, , drop = FALSE]
    if(nrow(awsDAT) == 0) return(NULL)
    temps <- temps[!ina]

    ivar <- names(awsDAT) %in% awsVAR$parameter_code
    tmp <- awsDAT[, ivar, drop = FALSE]

    if(nrow(tmp) == 0) return(NULL)

    ipar <- match(names(tmp), awsVAR$parameter_code)
    icol <- c('var_height', 'var_code', 'stat_code', 'multiplier')
    awsPAR <- awsVAR[ipar, icol, drop = FALSE]

    tmp <- lapply(seq_along(temps), function(j){
        val <- as.numeric(tmp[j, ])
        val <- val * awsPAR$multiplier
        data.frame(network = awsNET,
                id = awsID,
                height = awsPAR$var_height,
                var_code = awsPAR$var_code,
                stat_code = awsPAR$stat_code,
                obs_time = as.numeric(temps[j]),
                value = round(val, 4),
                limit_check = NA)
    })
    tmp <- do.call(rbind, tmp)

    fun_format <- list(as.integer, as.character, as.numeric, as.integer,
                       as.integer, as.integer, as.numeric, as.integer)
    tmp <- lapply(seq_along(fun_format), function(j) fun_format[[j]](tmp[[j]]))
    tmp <- as.data.frame(tmp)
    names(tmp) <- c("network", "id", "height", "var_code",
                    "stat_code", "obs_time", "value", "limit_check")

    return(tmp)
}
