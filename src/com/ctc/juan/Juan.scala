package com.ctc.juan

case class Juan(val path:String,val name:String)

object Juan{
  val HOLIDAY_JUAN = Juan("juan/holiday","holiday_juan")
  val WORKDAY_JUAN = Juan("juan/workday","workday_juan")
  val PRODUCT_JUAN = Juan("juan/product","product_juan")
  val ACCT_JUAN = Juan("juan/acct","acct_juan")
  val CERT_JUAN = Juan("juan/cert","cert_juan")
  val ENSEMBLE_JUAN = Juan("juan/ensemble","juan")

  val LPA_WORK_JUAN = Juan("juan/work_lpa","lpa_juan")
  val LPA_FREE_JUAN = Juan("juan/free_lpa","lpa_juan")
  val LPA_HOLIDAY_JUAN = Juan("juan/holiday_lpa","lpa_juan")
  val LPA_ENSEMBLE_JUAN = Juan("juan/ensemble_lpa","lpa_juan")
}