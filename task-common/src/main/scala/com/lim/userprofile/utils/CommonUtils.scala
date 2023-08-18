package com.lim.userprofile.utils

import com.lim.userprofile.constants.ConstCode

object CommonUtils {

  //根据类型加单引
  def checkQuota(tagValueType: String, subTagValue: String): String = {
    if (tagValueType == ConstCode.TAG_VALUE_TYPE_STRING
      || tagValueType == ConstCode.TAG_VALUE_TYPE_DATE) {
      "'" + subTagValue + "'"
    } else {
      subTagValue
    }
  }

  //根据标签的值类型来 定义字段类型
  def convertToHiveTableFieldType(tagValueType: String): String = {
    tagValueType match {
      case ConstCode.TAG_VALUE_TYPE_STRING => "STRING"
      case ConstCode.TAG_VALUE_TYPE_LONG => "BIGINT"
      case ConstCode.TAG_VALUE_TYPE_DECIMAL => "DECIMAL(16,2)"
      case ConstCode.TAG_VALUE_TYPE_DATE => "STRING"
    }
  }
}
