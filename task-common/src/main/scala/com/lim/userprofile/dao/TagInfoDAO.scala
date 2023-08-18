package com.lim.userprofile.dao

import com.lim.userprofile.bean.TagInfo
import com.lim.userprofile.utils.SqlUtils

object TagInfoDAO {
  def getTagInfoByTaskId(taskId: String): TagInfo = {
    val tagInfoSql: String =
      s"""select
         | id,
         | tag_code,
         | tag_name,
         | parent_tag_id,
         | tag_type,
         | tag_value_type,
         | tag_value_limit,
         | tag_task_id,
         | tag_comment,
         | create_time
         | from tag_info
         | where tag_task_id = $taskId limit 1""".stripMargin
    val tagInfoOpt: Option[TagInfo] =
      SqlUtils.queryOne(tagInfoSql, classOf[TagInfo], true)
    var tagInfo: TagInfo = null;
    if (tagInfoOpt.isDefined) {
      tagInfo = tagInfoOpt.get
    } else {
      throw new RuntimeException("no tag for task_id  : " + taskId)
    }
    tagInfo
  }
}
