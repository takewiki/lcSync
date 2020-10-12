#加载数据库区域-------
library(tsda)

#设置配置文件------

cfg_lc <- conn_config(config_file = '/home/hulilei/app/lcSync/cfg/conn_lc_test.R')
cfg_rds <- conn_config(config_file = '/home/hulilei/app/lcSync/cfg/conn_rds.R')
#定义辅助函数-------

#' 获取最大号
#'
#' @param cfg 配置文件 
#'
#' @return 返回值
#' @export
#'
#' @examples
#' local_max_version()
local_icitem_max_version <- function(cfg=cfg_lc) {
  conn <- conn_open(conn_config_info = cfg)
  sql <- paste0("select isnull(max(FVersion),0) as FVersion  from t_icitem_log")
  data <- sql_select(conn,sql)
  res <- data$FVersion + 1
  conn_close(conn)
  return(res)
}


local_icitem_pushToImg <- function(cfg=cfg_lc) {
  conn <- conn_open(conn_config_info = cfg)
sql_del <- paste0("truncate table t_icitem_img") 
tsda::sql_update(conn,sql_del)

sql_ins <- paste0("insert into t_icitem_img
select FItemID,FNumber,FName,FModel ,FChartNumber,FErpClsID  from t_icitem")
tsda::sql_update(conn,sql_ins)
conn_close(conn)
}


local_icitem_pushToDel <- function(cfg=cfg_lc) {
  conn <- conn_open(conn_config_info = cfg)
  sql_del <- paste0("truncate table t_icitem_Del") 
  tsda::sql_update(conn,sql_del)
  
  sql_ins <- paste0("insert into t_icitem_Del
select FItemID,FNumber,FName,FModel ,FChartNumber,FErpClsID  from t_icitem_img")
  tsda::sql_update(conn,sql_ins)
  conn_close(conn)
}


local_icitem_diff <- function(cfg=cfg_lc) {
  conn <- conn_open(conn_config_info = cfg)
 
  sql <- paste0("insert into t_icitem_log(FItemID,FNumber,FName,FModel ,FChartNumber,FErpClsID,FVersion)
select * ,dbo.func_icitem_max_ver() as FVersion  from rds_icitem_diff
")
  tsda::sql_update(conn,sql)
  conn_close(conn)

}


local_icitem_push <- function(cfg=cfg_lc,remote = cfg_rds){
  conn_local <- conn_open(conn_config_info = cfg)
  conn_remote <- conn_open(conn_config_info = remote)
  sql_sel <- paste0("select FItemID,FNumber,FName,FModel ,FChartNumber,FErpClsID  from t_icitem_log
where FIsDo = 0 ")
  data <- tsda::sql_select(conn_local,sql_sel)
  ncount =nrow(data)
  if(ncount >0){
    
    tsda::db_writeTable(conn = conn_remote,table_name = 'rds_icitem_input',r_object = data,append = T)
    #更新数据
    #1 删除已有数据
    sql_del <-paste0("delete from rds_icitem
where FItemID  in 
(select FItemID from rds_icitem_input)
")
    tsda::sql_update(conn_remote,sql_del)
    #2插入现有数据
    sql_ins <- paste0("insert into rds_icitem
select * from  rds_icitem_input")
    tsda::sql_update(conn_remote,sql_ins)
    #3删除暂存数据
    sql_truncate <- paste0("truncate table rds_icitem_input")
    tsda::sql_update(conn_remote,sql_truncate)
    #设置数据处理已完成
    sql_done <- paste0("update t_icitem_log set FIsDo = 1
where FIsDo = 0 ")
    tsda::sql_update(conn_local,sql_done)
    
    
    
  }
}


local_icitem_sync_auto <- function(cfg=cfg_lc,remote = cfg_rds) {
  #计算差异
  local_icitem_diff(cfg = cfg)
  #推送数据
  local_icitem_push(cfg = cfg,remote = remote)
  #删除数据
  local_icitem_pushToDel(cfg = cfg)
  #删除镜像
  local_icitem_pushToImg(cfg = cfg)
  
}

