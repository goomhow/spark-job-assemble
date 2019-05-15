package com.ctc.backup

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.DecimalType
import com.ctc.util.INSTANTCE._
import com.ctc.util.HDFSUtil._

class BackUpModelData (@transient spark:SparkSession) extends Serializable{
  def getCdmaMonBase(month:String):DataFrame={
    val path = CDMA_DATA_PATH.format(month)
    if(!exists(path)){
      val t_sql =s"""SELECT
	A .Prd_Inst_Id,
	A .Accs_Nbr,
	A .Serv_Type_Id,
	A .Device_Term_Type_Id,
	A .Country_Flag,
	A .Pay_Mode_Id,
	A .Billing_Type_Id,
	A .Acct_Id,
	A .Cust_Id,
	A .Sub_Strategy_Segment_Id,
	A .Cde_Cluster_Type_Id,
	A .Age,
	A .Gender_Id,
	A .Exchange_Id,
	A .Ofr_Id,
	A .Ofr_Inst_Id,
	A .Acct_Open_Date,
	A .Innet_Dur,
	A .Old_Prd_Inst_Type_Id,
	A .Latn_Id,
	A .Group_Region_Id,
	cur.Cert_Nbr,
	A .Vip_Flag,
	A .Cust_Type_Id,
	A .Market_Manager_Id,
	A .Indus_Type_Id,
	A .Centrex_Flag,
	A .User_Type_Id,
	b3.Inv_Amt,
	b3.Total_Flux,
	b3.Std_Prd_Inst_Stat_Id,
	c3.O_Call_Dur,
	c3.O_Call_Cnt,
	c3.O_Call_Dstn,
	c3.T_Call_Dur,
	c3.T_Call_Cnt,
	c3.T_Call_Dstn,
	c3.Inner_Rgn_Amt,
	c3.O_Inner_Rgn_Cnt,
	c3.O_Inner_Rgn_Dur,
	c3.Inter_Rgn_Amt,
	c3.O_Inter_Rgn_Cnt,
	c3.O_Inter_Rgn_Dur,
	c3.O_Tol_Dur,
	c3.O_Tol_Cnt,
	c3.O_Tol_Dstn,
	c3.O_Inet_Pp_Sms_Cnt,
	c3.O_Onet_Pp_Sms_Cnt,
	c3.T_Inet_Pp_Sms_Cnt,
	c3.T_Onet_Pp_Sms_Cnt,
	c3.O_Sp_Sms_Cnt,
	c3.T_Sp_Sms_Cnt,
	c3.Sp_Sms_Amt,
	c3.Pp_Sms_Amt,
	c3.Int_Sms_Amt,
	d3.Recv_1X_Flux + d3.Send_1X_Flux AS F1X_flux,
	d3.Bil_1X_Dur AS Bil_1X_Dur,
	d3.Total_1X_Cnt AS Total_1X_Cnt,
	d3.Recv_3G_Flux + d3.Send_3G_Flux AS F3G_flux,
	d3.Bil_3G_Dur AS Bil_3G_Dur,
	d3.Total_3G_Cnt AS Total_3G_Cnt,
	d3.Tdd_Flux AS Tdd_Flux,
	d3.Tdd_Bil_Dur AS Tdd_Bil_Dur,
	d3.Total_Tdd_Cnt AS Total_Tdd_Cnt,
	d3.Wday_1X_Surf_Days + d3.Wday_3G_Surf_Days + d3.Wday_4G_Surf_Days AS Wday_Days,
	d3.Hday_1X_Surf_Days + d3.Hday_3G_Surf_Days + d3.Hday_4G_Surf_Days AS Hday_Days,
	d3.Wday_1X_Surf_Flux + d3.Wday_3G_Surf_Flux + d3.Wday_4G_Surf_Flux AS Wday_Flux,
	d3.Hday_1X_Surf_Flux + d3.Hday_3G_Surf_Flux + d3.Hday_4G_Surf_Flux AS Hday_Flux,
	d3.Wday_1X_Surf_Dur + d3.Wday_3G_Surf_Dur + d3.Wday_4G_Surf_Dur AS Wday_Dur,
	d3.Hday_1X_Surf_Dur + d3.Hday_3G_Surf_Dur + d3.Hday_4G_Surf_Dur AS Hday_Dur,
	(
		d3.Mo_20_Call_Flux + d3.Mo_21_Call_Flux + d3.Mo_22_Call_Flux + d3.Mo_23_Call_Flux + d3.Mo_0_Call_Flux + d3.Mo_1_Call_Flux + d3.Mo_2_Call_Flux + d3.Mo_3_Call_Flux + d3.Mo_4_Call_Flux + d3.Mo_5_Call_Flux
	) AS Home_Flux,
	(
		d3.Mo_6_Call_Flux + d3.Mo_7_Call_Flux
	) AS On_Flux,
	(
		d3.Mo_18_Call_Flux + d3.Mo_19_Call_Flux
	) AS Off_Flux,
	(
		d3.Mo_8_Call_Flux + d3.Mo_9_Call_Flux + d3.Mo_10_Call_Flux + d3.Mo_11_Call_Flux + d3.Mo_12_Call_Flux + d3.Mo_13_Call_Flux + d3.Mo_14_Call_Flux + d3.Mo_15_Call_Flux + d3.Mo_16_Call_Flux + d3.Mo_17_Call_Flux
	) AS Office_Flux,
	(
		d3.Mo_20_Call_Dur + d3.Mo_21_Call_Dur + d3.Mo_22_Call_Dur + d3.Mo_23_Call_Dur + d3.Mo_0_Call_Dur + d3.Mo_1_Call_Dur + d3.Mo_2_Call_Dur + d3.Mo_3_Call_Dur + d3.Mo_4_Call_Dur + d3.Mo_5_Call_Dur
	) AS Home_Dur,
	(
		d3.Mo_6_Call_Dur + d3.Mo_7_Call_Dur
	) AS On_Dur,
	(
		d3.Mo_18_Call_Dur + d3.Mo_19_Call_Dur
	) AS Off_Dur,
	(
		d3.Mo_8_Call_Dur + d3.Mo_9_Call_Dur + d3.Mo_10_Call_Dur + d3.Mo_11_Call_Dur + d3.Mo_12_Call_Dur + d3.Mo_13_Call_Dur + d3.Mo_14_Call_Dur + d3.Mo_15_Call_Dur + d3.Mo_16_Call_Dur + d3.Mo_17_Call_Dur
	) AS Office_Dur,
	s.owe_amt,
	f3.BALANCE,
	r.Extrem_Base_Flux,
	r.Pack_Flag,
	r.Pack_Cnt,
	r.Extrem_Pack_Flux,
	r.Extrem_Flux,
	r.Out_Exact_Flux,
	r.Exact_Flux,
	r.In_Exact_Flux,
	r.In_Base_Exact_Flux,
	r.In_Pack_Exact_Flux,
	s.Owe_Dur,
	s.Fin_Owe_Amt,
	s.Bill_Owe_Amt,
	T .charge_2809,
	T .charge_before,
	T .charge_ft,
	T .charge_ft_before,
	U .PAY_CHARGE
FROM
	(
		SELECT
			*
		FROM
			PV_MART_Z.BAS_PRD_INST_month
		WHERE
			Std_Prd_Id / 10000 = 1015
		AND billing_cycle_id = ${month}
	) A
LEFT JOIN (
	sel *
	FROM
		pv_mart_g.BAS_PRD_INST_CUR
) cur ON A .prd_inst_id = cur.prd_inst_id
LEFT JOIN (
	SELECT
		*
	FROM
		pv_mart_z.bas_mob_term_str_month
	WHERE
		billing_cycle_id = ${month}
) b3 ON A .prd_inst_id = b3.prd_inst_id
LEFT JOIN (
	SELECT
		*
	FROM
		pv_mart_z.PRD_SERV_APP_ALL_MON
	WHERE
		billing_cycle_id = ${month}
) c3 ON b3.prd_inst_id = c3.prd_inst_id
LEFT JOIN (
	SELECT
		*
	FROM
		PV_MART_Z.BEH_MOB_STR_MON
	WHERE
		billing_cycle_id = ${month}
) d3 ON b3.prd_inst_id = d3.prd_inst_id
LEFT JOIN (
	sel T .PROD_INST_ID,
	SUM (BALANCE) AS BALANCE
FROM
	pv_mart_z.bwt_fin_balance_m T
WHERE
	MONTH_ID = ${month}
GROUP BY
	PROD_INST_ID
) f3 ON b3.prd_inst_id = f3.PROD_INST_ID
LEFT JOIN (
	sel *
	FROM
		PV_MART_Z.BEH_MOB_STR_EXT_MON
	WHERE
		billing_cycle_id = ${month}
) r ON A .prd_inst_id = r.prd_inst_id
LEFT JOIN (
	sel *
	FROM
		PV_MART_g.BAS_PRD_INST_OWE_MONTH
	WHERE
		Billing_Cycle_Id = ${month}
) s ON A .prd_inst_id = s.prd_inst_id
LEFT JOIN (
	sel Prd_Inst_Id,
	SUM (
		CASE
		WHEN CHRG_SRC_CD IN (
			'2',
			'9',
			'22',
			'23',
			'24',
			'25',
			'1'
		) THEN
			charge
		ELSE
			0
		END
	) charge_2809,
	SUM (
		CASE
		WHEN CHRG_SRC_CD IN ('2', '9', '22', '24') THEN
			charge
		ELSE
			0
		END
	) charge_before,
	SUM (
		CASE
		WHEN data_type = 'APPORTION'
		AND CHRG_SRC_CD IN ('2', '9', '22', '23', '24') THEN
			charge
		ELSE
			0
		END
	) charge_ft,
	SUM (
		CASE
		WHEN data_type = 'APPORTION'
		AND CHRG_SRC_CD IN ('2', '9', '22', '24') THEN
			charge
		ELSE
			0
		END
	) charge_ft_before
FROM
	PV_DATA_Z.BIL_ACCT_ITEM_DETAIL_FIN
WHERE
	Billing_Cycle_Id = ${month}
GROUP BY
	Prd_Inst_Id
) T ON A .prd_inst_id = T .prd_inst_id
LEFT JOIN (
	sel acct_id,
	SUM (PAY_CHARGE) PAY_CHARGE
FROM
	PV_MART_Z.BWT_FIN_PAY_M
WHERE
	MONTH_ID = ${month}
GROUP BY
	acct_id
) U ON A .acct_id = U .acct_id"""
      spark.read.jdbc(TD_URL,s"($t_sql)x","Age",0,200,20,TD_PROPERTY).write.mode("overwrite").parquet(path)
    }
    spark.read.parquet(path)
  }
  def getBroadbandMonBase(month:String):DataFrame={
    val path = BD_DATA_PATH.format(month)
    if(!exists(path)){
      val t_sql =s"""sel T .Prd_Inst_Id,
 T .Billing_Cycle_Id,
 T .Accs_Nbr,
 T .Serv_Type_Id,
 T .Cde_Prd_Inst_Device_Stat_Id,
 T .Cde_Prd_Inst_Stat_Id,
 T .Std_Prd_Inst_Stat_Id,
 T .Prd_Id,
 T .Std_Prd_Id,
 T .Country_Flag,
 T .User_Type_Id,
 T .Pay_Mode_Id,
 T .Strategy_Segment_Id,
 T .Acct_Id,
 T .Cust_Id,
 T .Age,
 T .Vip_Flag,
 T .Cust_Type_Id,
 T .Market_Manager_Id,
 T .Indus_Type_Id,
 T .Centrex_Flag,
 T .Line_Rate,
 T .Exchange_Id,
 T .Serv_Order_Id,
 T .Ofr_Id,
 T .Ofr_Inst_Id,
 T .Acct_Open_Date,
 T .Eff_Acct_Month,
 T .Exp_Acct_Month,
 T .Innet_Billing_Cycle_Id,
 T .Outnet_Billing_Cycle_Id,
 T .Stop_Billing_Cycle_Id,
 T .Remove_Type_Id,
 T .Innet_Dur,
 T .Innet_Dur_Lvl_Id,
 T .Old_Prd_Inst_Type_Id,
 T .Merge_Prom_Id,
 T .Stop_Dur,
 T .Bil_Flag,
 T .Innet_Flag,
 T .Prd_Inst_Flag,
 T .Prd_Inst_Nbr,
 T .Sub_Bureau_Id,
 T .Latn_Id,
 T .Cty_Reg_Flg,
 T .Group_Region_Id,
 T .Vip_Manager_Id,
 T .Vip_Manager_Name,
 T .Grid_Id,
 T .Act_Flag,
 A .Cert_Type_Id,
 A .Cert_Type_Name,
 A .Cert_Nbr,
 A .User_Name,
 A .User_Address,
 A .Contact_Info,
 A .Cust_Name,
 A .Gender_Id,
 A .Gender_Name,
 A .Ofr_Name,
 b.Send_Rate,
 b.Recv_Rate,
 b.Over_Per_Hour_Amt,
 b.Limit_Hour_Flag,
 b.Available_Hours,
 b.Prtl_Mons,
 b.Addr_Info,
 b.Avg_Mon_Amt,
 b.Prtl_Amt,
 b.End_Billing_Cycle_Id,
 b.End_Date,
 b.Pay_Flag,
 b.Pro_Prtl_Mons,
 b.Pricing_Plan_Id,
 b.Pricing_Desc,
 b.Unit_Charge,
 b.Cycle_Charge,
 b.Last_Renewal_Date,
 b.Bef_Renew_Exp_Month,
 b.Last_Renewal_Month,
 b.Ref_Type,
 b.Assess_Bil_Flag,
 b.Recv_Amt,
 b.Broadband_Ticket_Flag,
 b.Accs_Mode_Cd,
 b.Acc_Type,
 c.charge_2809,
 c.charge_before,
 c.charge_ft,
 c.charge_ft_before,
 D .Acct_Balance_Amount,
 E .Owe_Dur,
 E .Owe_Amt,
 E .Fin_Owe_Amt,
 E .Bill_Owe_Amt,
 f.PAY_CHARGE,
 G .Wday_Hh9_15_Brd_Bnd_Dur,
 G .Wday_Hh9_15_Brd_Bnd_Cnt,
 G .Wday_Hh19_22_Brd_Bnd_Dur,
 G .Wday_Hh19_22_Brd_Bnd_Cnt,
 G .Wday_Hh15_19_Brd_Bnd_Dur,
 G .Wday_Hh15_19_Brd_Bnd_Cnt,
 G .Wday_Brd_Bnd_Dur,
 G .Wday_Brd_Bnd_Days,
 G .Up_Wday_Hh9_15_Brd_Bnd_Flux,
 G .Up_Wday_Hh19_22_Brd_Bnd_Flux,
 G .Up_Wday_Hh15_19_Brd_Bnd_Flux,
 G .Up_Hh22_9_Brd_Bnd_Flux,
 G .Up_Hday_Hh9_15_Brd_Bnd_Flux,
 G .Up_Hday_Hh19_22_Brd_Bnd_Flux,
 G .Up_Hday_Hh15_19_Brd_Bnd_Flux,
 G .Uload_Brd_Bnd_Bytes,
 G .Stud_Dur,
 G .Stud_Days,
 G .Movie_Dur,
 G .Movie_Days,
 G .Kvirus_Dur,
 G .Kvirus_Days,
 G .Hh22_9_Brd_Bnd_Dur,
 G .Hh22_9_Brd_Bnd_Cnt,
 G .Hday_Hh9_15_Brd_Bnd_Dur,
 G .Hday_Hh9_15_Brd_Bnd_Cnt,
 G .Hday_Hh19_22_Brd_Bnd_Dur,
 G .Hday_Hh19_22_Brd_Bnd_Cnt,
 G .Hday_Hh15_19_Brd_Bnd_Dur,
 G .Hday_Hh15_19_Brd_Bnd_Cnt,
 G .Hday_Brd_Bnd_Dur,
 G .Hday_Brd_Bnd_Days,
 G .Game_Dur,
 G .Game_Days,
 G .Down_Wday_Hh9_15_Brd_Bnd_Flux,
 G .Down_Wday_Hh19_22_Brd_Bnd_Flux,
 G .Down_Wday_Hh15_19_Brd_Bnd_Flux,
 G .Down_Hh22_9_Brd_Bnd_Flux,
 G .Down_Hday_Hh9_15_Brd_Bnd_Flux,
 G .Down_Hday_Hh19_22_Brd_Bnd_Flux,
 G .Down_Hday_Hh15_19_Brd_Bnd_Flux,
 G .Dload_Brd_Bnd_Bytes,
 G .Brd_Bnd_Dur
FROM
	PV_MART_z.BAS_PRD_INST_month T
LEFT JOIN pv_mart_z.BAS_PRD_INST_CUR A ON T .prd_inst_id = A .prd_inst_id
LEFT JOIN pv_mart_z.BAS_SERV_BRD_MON b ON T .prd_inst_id = b.prd_inst_id
AND b.Billing_Cycle_Id = ${month}
LEFT JOIN (
	sel Prd_Inst_Id,
	SUM (
		CASE
		WHEN CHRG_SRC_CD IN (
			'2',
			'9',
			'22',
			'23',
			'24',
			'25',
			'1'
		) THEN
			charge
		ELSE
			0
		END
	) charge_2809,
	SUM (
		CASE
		WHEN CHRG_SRC_CD IN ('2', '9', '22', '24') THEN
			charge
		ELSE
			0
		END
	) charge_before,
	SUM (
		CASE
		WHEN data_type = 'APPORTION'
		AND CHRG_SRC_CD IN ('2', '9', '22', '23', '24') THEN
			charge
		ELSE
			0
		END
	) charge_ft,
	SUM (
		CASE
		WHEN data_type = 'APPORTION'
		AND CHRG_SRC_CD IN ('2', '9', '22', '24') THEN
			charge
		ELSE
			0
		END
	) charge_ft_before
FROM
	PV_DATA_z.BIL_ACCT_ITEM_DETAIL_FIN
WHERE
	Billing_Cycle_Id = ${month}
GROUP BY
	Prd_Inst_Id
) c ON T .Prd_Inst_Id = c.Prd_Inst_Id
LEFT JOIN (
	sel Prd_Inst_Id,
	SUM (Acct_Balance_Amount) Acct_Balance_Amount
FROM
	PV_MART_z.BAS_PRD_BALANCE_MON
WHERE
	Billing_Cycle_Id = ${month}
GROUP BY
	Prd_Inst_Id
) D ON T .Prd_Inst_Id = D .Prd_Inst_Id
LEFT JOIN (
	sel Prd_Inst_Id,
	Owe_Dur,
	Owe_Amt,
	Fin_Owe_Amt,
	Bill_Owe_Amt
FROM
	PV_MART_z.BAS_PRD_INST_OWE_MONTH
WHERE
	Billing_Cycle_Id = ${month}
) E ON T .Prd_Inst_Id = E .Prd_Inst_Id
LEFT JOIN (
	sel acct_id,
	SUM (PAY_CHARGE) PAY_CHARGE
FROM
	PV_MART_z.BWT_FIN_PAY_M
WHERE
	MONTH_ID = ${month}
GROUP BY
	acct_id
) f ON T .acct_id = f.acct_id
LEFT JOIN PV_MART_Z.BAS_PRD_INST_BRD_BEH_MONTH G ON T .Prd_Inst_Id = G .Prd_Inst_Id
AND G .Billing_Cycle_Id = ${month}
WHERE
	T .Billing_Cycle_Id = ${month}
AND T .Std_Prd_Id / 10000 = 3020"""
      spark.read.jdbc(TD_URL,s"($t_sql)x","Age",0,200,20,TD_PROPERTY).write.mode("overwrite").parquet(path)
    }
    spark.read.parquet(path)
  }

  def getMergePrdInstId(month:String): Unit ={
    val path = PRD_PRD_INST_EXT_MON.format(month)
    val t_sql= s"""sel latn_id,prd_inst_id
                 FROM
                 	pv_data_z.PRD_PRD_INST_EXT_mon
                 WHERE
                 	Std_Merge_Prom_Type_Id IN (101, 102)
                 AND billing_cycle_id = ${month}
                 AND (
                 	Cde_Merge_Prom_Name LIKE '%十全十美%'
                 	OR Cde_Merge_Prom_Name LIKE '%不限量%'
                 )"""
    spark.read.jdbc(TD_URL,s"($t_sql)a","latn_id",1001,1020,19,TD_PROPERTY).write.mode("overwrite").parquet(path)
  }
}
