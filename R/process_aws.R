
#' TAHMO replicator.
#'
#' Download data from TAHMO API.
#' 
#' @param aws_dir full path to the directory TAHMO containing the AWS_DATA folder.
#' @param adt_dir full path to the directory containing the AWS_DATA folder for ADT.
#' 
#' @export

replicator.tahmo <- function(aws_dir, adt_dir){
    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "TAHMO")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    dirLOG1 <- file.path(adt_dir, "AWS_DATA", "LOG", "TAHMO")
    if(!dir.exists(dirLOG1))
        dir.create(dirLOG1, showWarnings = FALSE, recursive = TRUE)

    Sys.setenv(TZ = "Africa/Blantyre")
    mon <- format(Sys.time(), '%Y%m')
    logDOWN <- file.path(dirLOG, paste0("download_tahmo_", mon, ".txt"))

    ret <- try(get.tahmo.api(aws_dir, adt_dir), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        mserr <- gsub('[\r\n]', '', ret[1])
        msg <- "Downloading TAHMO data failed"
        format.out.msg(paste(mserr, '\n', msg), logDOWN)
        fileLog <- file.path(dirLOG1, basename(logDOWN))
        file.copy(logDOWN, fileLog)
        return(2)
    }

    return(0)
}

#' Process TAHMO data.
#'
#' Get the data from TAHMO, parse and convert into ADT format.
#' 
#' @param aws_dir full path to the directory TAHMO containing the AWS_DATA folder.
#' @param adt_dir full path to the directory containing the AWS_DATA folder for ADT.
#' 
#' @export

process.tahmo <- function(aws_dir, adt_dir){
    dirLOG <- file.path(adt_dir, "AWS_DATA", "LOG", "TAHMO")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)

    Sys.setenv(TZ = "Africa/Blantyre")
    mon <- format(Sys.time(), '%Y%m')
    logPROC <- file.path(dirLOG, paste0("processing_tahmo_", mon, ".txt"))

    ret <- try(get.tahmo.data(aws_dir, adt_dir), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        mserr <- gsub('[\r\n]', '', ret[1])
        msg <- "Getting TAHMO data failed"
        format.out.msg(paste(mserr, '\n', msg), logPROC)
        return(2)
    }

    return(0)
}

#' Process CAMPBELL SCIENTIFIC data.
#'
#' Get the data from CAMPBELL SCIENTIFIC database, parse and convert into ADT format.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder on CAMPBELL server.
#' @param adt_dir full path to the directory containing the AWS_DATA folder on ADT server.
#' 
#' @export

process.campbell <- function(aws_dir, adt_dir){
    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "CAMPBELL")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)

    Sys.setenv(TZ = "Africa/Blantyre")
    mon <- format(Sys.time(), '%Y%m')
    logPROC <- file.path(dirLOG, paste0("processing_campbell_", mon, ".txt"))

    ret <- try(get.campbell.data(aws_dir, adt_dir), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        mserr <- gsub('[\r\n]', '', ret[1])
        msg <- "Getting CAMPBELL data failed"
        format.out.msg(paste(mserr, '\n', msg), logPROC)
        return(2)
    }

    return(0)
}
