package com.lim.userprofile.utils

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtils {

  def load(propertieName:String): Properties ={
    val prop=new Properties();
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.
      getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }

}