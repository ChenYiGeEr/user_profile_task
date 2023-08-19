package com.lim.userprofile.constants

object ConstCode {

  /** hive和clickhouse宽表前缀 */
  val TABLE_NAME_PREFIX="up_tag_merge_"
  /** clickhouse标签位图表前缀 */
  val CLICKHOUSE_BITMAP_TAG_TABLE_PREFIX: String = "user_tag_value_"

  val TAG_VALUE_TYPE_LONG="1"
  val TAG_VALUE_TYPE_DECIMAL="2"
  val TAG_VALUE_TYPE_STRING="3"
  val TAG_VALUE_TYPE_DATE="4"

  val TASK_PROCESS_SUCCESS="1"
  val TASK_PROCESS_ERROR="2"

  val TASK_STAGE_START="1"
  val TASK_STAGE_RUNNING="2"

}