package com.lim.userprofile.dao

import com.lim.userprofile.bean.TaskInfo
import com.lim.userprofile.utils.SqlUtils

object TaskInfoDAO {

  def getTaskInfo(taskId: String): TaskInfo = {
    val taskInfoSql: String =
      s"""select
         | id,
         | task_name,
         | task_status,
         | task_comment,
         | task_time ,
         | task_type,
         | exec_type,
         | main_class,
         | file_id,
         | task_args,
         | task_sql,
         | task_exec_level,
         | create_time
         | from task_info where id= $taskId limit 1""".stripMargin
    val taskInfoOpt: Option[TaskInfo] =
      SqlUtils.queryOne(taskInfoSql, classOf[TaskInfo], true)
    var taskInfo: TaskInfo = null;
    if (taskInfoOpt.isDefined) {
      taskInfo = taskInfoOpt.get
    } else {
      throw new RuntimeException("no task for task_id  : " + taskId)
    }
    taskInfo
  }

}
