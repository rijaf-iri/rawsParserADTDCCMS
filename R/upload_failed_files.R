#' Re-upload files failed to upload.
#'
#' Re-upload all files failed to upload from the previous task to ADT server.
#' 
#' @param awsNET name of the AWS network from \code{adt_network} table.
#' @param dirAWS full path to the directory containing the AWS_DATA folder.
#' @param dirUP full path to the directory containing the AWS_DATA folder on ADT server.
#' 
#' @export

upload.failed.files <- function(awsNET, dirAWS, dirUP){
    on.exit({
        if(upload) ssh::ssh_disconnect(session)
    })
    Sys.setenv(TZ = "Africa/Blantyre")
    mon <- format(Sys.time(), '%Y%m')

    dirLOCData <- file.path(dirAWS, "AWS_DATA", "DATA", awsNET)
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LOG", awsNET)
    logPROC <- paste0("processing_", tolower(awsNET), "_", mon, ".txt")
    logPROC <- file.path(dirLOG, logPROC)
    uploadFailed <- file.path(dirLOG, 'upload_failed.txt')

    session <- connect.ssh(dirAWS)
    if(is.null(session)){
        msg <- "Unable to connect to ADT server"
        format.out.msg(msg, logPROC)
        upload <- FALSE
    }else{
        dirUPData <- file.path(dirUP, "AWS_DATA", "DATA", "minutes", awsNET)
        delayedFiles <- file.path(dirUP, "AWS_DATA", "LOG", awsNET, "delayed_data.txt")
        upload <- TRUE
    }

    if(!file.exists(uploadFailed)) return(0)

    failedFILES <- readLines(uploadFailed, warn = FALSE)
    failedFILES <- trimws(failedFILES)
    failedFILES <- failedFILES[failedFILES != ""]

    if(length(failedFILES) == 0) return(0)

    failedFILES1 <- failedFILES

    for(uFile in failedFILES){
        locFile <- file.path(dirLOCData, uFile)
        upFile <- file.path(dirUPData, uFile)
        if(upload){
            ret <- try(ssh::scp_upload(session, locFile, to = upFile, verbose = FALSE), silent = TRUE)
            if(inherits(ret, "try-error")){
                if(grepl('disconnected', ret[1])){
                    Sys.sleep(5)
                    session <- connect.ssh(dirAWS)
                    upload <- if(is.null(session)) FALSE else TRUE
                }
            }else{
                ## send uploaded file to ADT to update aws data table on ADT sever, 
                ## scripts to update aws_minutes, qc limit check, 
                ## qc spatial check, update aws_hourly and aws_daily
                ssh::ssh_exec_wait(session, command = c(
                    paste0("echo '", uFile, "' >> ", delayedFiles)
                ))

                failedFILES1 <- failedFILES1[failedFILES1 != uFile]
                if(length(failedFILES1) > 0 | failedFILES[length(failedFILES)] == uFile){
                    cat(failedFILES1, file = uploadFailed, sep = '\n', append = FALSE)
                }
            }
        }
    }

    conLogFile <- file.path(dirAWS, "AWS_DATA", "LOG", "log_error_connection.txt")
    if(file.exists(conLogFile)){
        if(upload){
            upLogDir <- file.path(dirUP, "AWS_DATA", "LOG", "CONNECTION")
            ssh::ssh_exec_wait(session, command = c(
                paste0('if [ ! -d ', upLogDir, ' ] ; then mkdir -p ', upLogDir, ' ; fi')
            ))

            upFile <- paste0('log_error_connection_', awsNET, '_', format(Sys.time(), '%Y%m%d%H%M%S'), '.txt')
            upFile <- file.path(upLogDir, upFile)
            ssh::scp_upload(session, conLogFile, to = upFile, verbose = FALSE)
            unlink(conLogFile)
        }
    }

    return(0)
}
