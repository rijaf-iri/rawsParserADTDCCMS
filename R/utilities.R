
connect.DBI <- function(con_args, drv, dirAWS){
    args <- c(list(drv = drv), con_args)
    con <- try(do.call(DBI::dbConnect, args), silent = TRUE)
    if(inherits(con, "try-error")){
        conLogFile <- file.path(dirAWS, "AWS_DATA", "LOG", "log_error_connection.txt")
        err <- gsub('[\r\n]', '', con)
        drv_info <- paste0('Driver: ', class(drv)[1],
                           '; Package: ', attributes(class(drv))$package, '\n')
        msg <- paste("Unable to connect to the database\n", drv_info, err)
        format.out.msg(msg, conLogFile)
        return(NULL)
    }

    return(con)
}

connect.RODBC <- function(con_args, dirAWS){
    args <- paste0(names(con_args), '=', unlist(con_args))
    args <- paste(args, collapse = ";")
    args <- list(connection = args, readOnlyOptimize = TRUE)
    con <- try(do.call(RODBC::odbcDriverConnect, args), silent = TRUE)
    if(inherits(con, "try-error")){
        conLogFile <- file.path(dirAWS, "AWS_DATA", "LOG", "log_error_connection.txt")
        err <- gsub('[\r\n]', '', con)
        msg <- paste("Unable to connect to the database\n", err)
        format.out.msg(msg, conLogFile)
        return(NULL)
    }

    return(con)
}

connect.adt_db <- function(dirAWS){
    ff <- file.path(dirAWS, "AWS_DATA", "AUTH", "adt.con")
    adt <- readRDS(ff)
    conn <- connect.DBI(adt$connection, RMySQL::MySQL(), dirAWS)
    if(is.null(conn)){
        Sys.sleep(3)
        conn <- connect.DBI(adt$connection, RMySQL::MySQL(), dirAWS)
        if(is.null(conn)) return(NULL)
    }
    DBI::dbExecute(conn, "SET GLOBAL local_infile=1")

    return(conn)
}

connect.campbell <- function(dirAWS){
    ff <- file.path(dirAWS, "AWS_DATA", "AUTH", "campbell.con")
    campbell <- readRDS(ff)
    conn <- connect.RODBC(campbell$connection, dirAWS)
    if(is.null(conn)){
        Sys.sleep(3)
        conn <- connect.RODBC(campbell$connection, dirAWS)
        if(is.null(conn)) return(NULL)
    }

    return(conn)
}

tahmo.api <- function(aws_dir){
   ff <- file.path(aws_dir, "AWS_DATA", "AUTH", "tahmo.api")
   readRDS(ff)
}

connect.ssh <- function(aws_dir){
    ff <- file.path(aws_dir, "AWS_DATA", "AUTH", "adt.cred")
    ssh <- readRDS(ff)
    session <- try(do.call(ssh::ssh_connect, ssh$cred), silent = TRUE)
    if(inherits(session, "try-error")){
        conLogFile <- file.path(aws_dir, "AWS_DATA", "LOG", "log_error_connection.txt")
        err <- gsub('[\r\n]', '', session)
        msg <- paste("SSH: unable to connect to ADT server\n", err)
        format.out.msg(msg, conLogFile)
        return(NULL)
    }

    return(session)
}

format.out.msg <- function(msg, logfile, append = TRUE){
    ret <- c(paste("Time:", Sys.time(), "\n"), msg, "\n",
             "*********************************\n")
    cat(ret, file = logfile, append = append)
}
