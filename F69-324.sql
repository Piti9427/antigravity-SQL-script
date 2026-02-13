
-- ALTER SESSION ENABLE PARALLEL QUERY;
-- ALTER SESSION ENABLE PARALLEL DML;

WITH
/* =========================================================
 CTE 0: Scope List (ACC_NO only)
 ========================================================= */
scope_list AS (
  SELECT /*+ MATERIALIZE PARALLEL(64) */
    ACC_NO
  FROM PGETMP.LIST_RECAL_F69_324
  WHERE ACC_NO IN ('1000062163', '1000085783')
),

/* =========================================================
 1) bill_ver: หา Version ล่าสุด
 ========================================================= */
bill_ver AS (
  SELECT /*+ MATERIALIZE PARALLEL(b 64) */
    b.account_no,
    MAX(b.version) AS pick_version
  FROM DMS.DMS_TRN_BILL_SUMMARY b
  WHERE EXISTS (SELECT 1 FROM scope_list s WHERE s.ACC_NO = b.account_no)
  GROUP BY b.account_no
),
bills_v1 AS (
  SELECT
    /*+ MATERIALIZE PARALLEL(64) */
    account_no,
    SUM(NVL(INTEREST_CAL_AMOUNT, 0)) AS ACCRUED_INTEREST,
    SUM(NVL(FINE_CAL_AMOUNT, 0)) AS ACCRUED_FINE
  FROM DMS.DMS_TRN_BILLS
  WHERE version = 1
    AND STATUS = '1'
    AND DUE_PERIOD < TRUNC(SYSDATE)
    AND EXISTS (
      SELECT 1
      FROM scope_list s
      WHERE s.ACC_NO = account_no
    )
  GROUP BY account_no
),
bills_v2 AS (
  SELECT
    /*+ MATERIALIZE PARALLEL(64) */
    account_no,
    SUM(NVL(INTEREST_CAL_AMOUNT, 0)) AS ACCRUED_INTEREST,
    SUM(NVL(FINE_CAL_AMOUNT, 0)) AS ACCRUED_FINE
  FROM DMS.DMS_TRN_BILLS
  WHERE version = 2
    AND STATUS = '1'
    AND DUE_PERIOD < TRUNC(SYSDATE)
    AND EXISTS (
      SELECT 1
      FROM scope_list s
      WHERE s.ACC_NO = account_no
    )
  GROUP BY account_no
),
/* =========================================================
 2) rsm_safe (เอา CID/CIF จากที่นี่เป็นหลัก)
 ========================================================= */
rsm_safe AS (
  SELECT /*+ MATERIALIZE PARALLEL(64) */
    ACC_NO,
    CID,
    CIF,
    LOAN_TYPE,
    STA_CODE,
    LOAN_ACC_STATUS,
    GROUP_FLAG,
    END_CONTRACT_DATE,
    NO_OF_LATE_BILL,
    LAST_PAYMENT_DATE,
    ENTITY,
    LOAN_CLASSIFICATION AS LOAN_CLASS_STR
  FROM DMSDBA.REPORT_SUMMARY_MONTH
  WHERE EXISTS (SELECT 1 FROM scope_list s WHERE s.ACC_NO = ACC_NO)
),

/* =========================================================
 3) contract_pick (ผูก CID จาก rsm_safe)
 ========================================================= */
contract_pick AS (
  SELECT /*+ MATERIALIZE PARALLEL(64) */
    cid,
    stu_bank_code
  FROM (
    SELECT /*+ PARALLEL(c 64) PARALLEL(rb2 64) */
      c.cid,
      c.stu_bank_code,
      ROW_NUMBER() OVER (
        PARTITION BY c.cid
        ORDER BY
          CASE WHEN rb2.bank_code IS NOT NULL THEN 0 ELSE 1 END,
          CASE WHEN c.stu_bank_code IS NOT NULL THEN 0 ELSE 1 END,
          c.rowid DESC
      ) rn
    FROM DMSDBA.CONTRACT c
    LEFT JOIN DMSDBA.RDBBANK rb2 ON c.stu_bank_code = rb2.bank_code
    WHERE EXISTS (SELECT 1 FROM rsm_safe r WHERE r.CID = c.CID)
  )
  WHERE rn = 1
),

/* =========================================================
 4) log_cal_max
 ========================================================= */
log_cal_max AS (
  SELECT /*+ MATERIALIZE PARALLEL(lc 64) */
    lc.account_no,
    MAX(lc.cal_date) AS max_cal_date
  FROM DMS.DMS_LOG_CAL lc
  WHERE EXISTS (SELECT 1 FROM scope_list s WHERE s.ACC_NO = lc.account_no)
  GROUP BY lc.account_no
),

/* =========================================================
 5) tbs_v1/tbs_v2 (กัน row แตก: สรุปให้เหลือ 1 แถวต่อ acc/version)
 ========================================================= */
tbs_v1 AS (
  SELECT /*+ MATERIALIZE PARALLEL(64) */
    account_no,
    SUM(NVL(CAPITAL_REMAIN,0))       AS CAPITAL_REMAIN,
    SUM(NVL(ACCRUED_INSTALLMENT,0))  AS ACCRUED_INSTALLMENT,
    SUM(NVL(ACCRUED_INTEREST,0))     AS ACCRUED_INTEREST,
    SUM(NVL(ACCRUED_FINE,0))         AS ACCRUED_FINE,
    SUM(NVL(CARRY_INTEREST,0))       AS CARRY_INTEREST,
    SUM(NVL(CARRY_FINE,0))           AS CARRY_FINE,
    CARRY_INTEREST_CAL,
    CARRY_FINE_CAL
  FROM DMS.DMS_TRN_BILL_SUMMARY
  WHERE version = 1
    AND EXISTS (SELECT 1 FROM scope_list s WHERE s.ACC_NO = account_no)
  GROUP BY account_no, CARRY_INTEREST_CAL, CARRY_FINE_CAL
),
tbs_v2 AS (
  SELECT /*+ MATERIALIZE PARALLEL(64) */
    account_no,
    SUM(NVL(CAPITAL_REMAIN,0))       AS CAPITAL_REMAIN,
    SUM(NVL(ACCRUED_INSTALLMENT,0))  AS ACCRUED_INSTALLMENT,
    SUM(NVL(ACCRUED_INTEREST,0))     AS ACCRUED_INTEREST,
    SUM(NVL(ACCRUED_FINE,0))         AS ACCRUED_FINE,
    SUM(NVL(CARRY_INTEREST,0))       AS CARRY_INTEREST,
    SUM(NVL(CARRY_FINE,0))           AS CARRY_FINE,
    CARRY_INTEREST_CAL,
    CARRY_FINE_CAL
  FROM DMS.DMS_TRN_BILL_SUMMARY
  WHERE version = 2
    AND EXISTS (SELECT 1 FROM scope_list s WHERE s.ACC_NO = account_no)
  GROUP BY account_no, CARRY_INTEREST_CAL, CARRY_FINE_CAL
),

/* =========================================================
 6) tid_pick (ถ้ามีหลายแถว เลือก/สรุปก่อน)
 ========================================================= */
tid_pick AS (
  SELECT /*+ MATERIALIZE PARALLEL(64) */
    account_no,
    SUM(NVL(INTEREST,0)) AS INTEREST,
    SUM(NVL(FINE,0))     AS FINE
  FROM DMS.DMS_TRN_CAL_INT_ADD_DAY
  WHERE EXISTS (SELECT 1 FROM scope_list s WHERE s.ACC_NO = account_no)
  GROUP BY account_no
),

/* =========================================================
 7) main_data
 ========================================================= */
main_data AS (
  SELECT /*+ QB_NAME(main) LEADING(bc) USE_HASH(bc rsm c rb ar pr la fr tid lc bv t1 t2 ktb) PARALLEL(64) */
    CASE
      WHEN rsm.ENTITY = 'KTB' THEN 'ธนาคารกรุงไทย จำกัด (มหาชน)'
      WHEN rsm.ENTITY = 'IBANK' THEN 'ธนาคารอิสลามแห่งประเทศไทย'
      ELSE NVL(rb.BANK_NAME_TH, '(ไม่พบธนาคาร)')
    END AS "ดูแลบัญชีโดย",

    bc.ACC_NO AS "เลขที่บัญชี",
    COALESCE(ar.LOAN_TYPE, rsm.LOAN_TYPE) AS "ประเภทบัญชี",
    rsm.LOAN_ACC_STATUS AS "รหัสสถานะบัญชี",

    CASE
      WHEN rsm.STA_CODE = '00' THEN 'ปกติ'
      WHEN rsm.STA_CODE = '01' THEN 'ตาย'
      WHEN rsm.STA_CODE = '02' THEN 'สาบสูญ'
      WHEN rsm.STA_CODE = '03' THEN 'ทุพพลภาพ'
      WHEN rsm.STA_CODE = '04' THEN 'โรคติดต่อร้ายแรง'
      WHEN rsm.STA_CODE = '05' THEN 'โรคเรื้อรัง'
      WHEN rsm.STA_CODE = '06' THEN 'จำคุกตลอดชีวิต'
      WHEN rsm.STA_CODE = '07' THEN 'ล้มละลาย'
      WHEN rsm.STA_CODE = '08' THEN 'พักชำระหนี้บัตรสวัสดิการแห่งรัฐ'
      ELSE 'ไม่ทราบสถานะ'
    END AS "สถานะบุคคล",

    CASE
      WHEN rsm.LOAN_ACC_STATUS = '00' THEN 'ปกติ (อยู่ระหว่างการผ่อนชำระ)'
      WHEN rsm.LOAN_ACC_STATUS = '01' THEN 'บัญชีใหม่'
      WHEN rsm.LOAN_ACC_STATUS = '90' THEN 'ปิดบัญชี (Pay Off)'
      WHEN rsm.LOAN_ACC_STATUS = '91' THEN 'ปิดบัญชี (รวมบัญชี)'
      WHEN rsm.LOAN_ACC_STATUS = '92' THEN 'ปิดบัญชี (ไม่มียอดหนี้ค้างชำระ)'
      WHEN rsm.LOAN_ACC_STATUS = '93' THEN 'ปิดบัญชี (มียอดหนี้ค้างชำระ)'
      WHEN rsm.LOAN_ACC_STATUS = '94' THEN 'ปิดบัญชี (จากการโอนหนี้)'
      WHEN rsm.LOAN_ACC_STATUS = '95' THEN 'ปิดบัญชี (เนื่องจากเสียชีวิต)'
      ELSE 'ไม่ทราบสถานะ'
    END AS "สถานะบัญชี",

    rsm.CIF AS "รหัสผู้กู้",
    rsm.CID AS "เลขประจำตัวประชาชน",

    COALESCE(ar.TITLE_NAME, pr.HR_TITLE) AS TITLE_NAME,
    COALESCE(ar.HR_NAME, pr.HR_NAME) AS HR_NAME,
    COALESCE(ar.HR_SURNAME, pr.HR_SURNAME) AS HR_SURNAME,

    rsm.LAST_PAYMENT_DATE,
    rsm.NO_OF_LATE_BILL,

    CASE
      WHEN rsm.LOAN_CLASS_STR = 'A1' THEN 'ปกติ'
      WHEN rsm.LOAN_CLASS_STR = 'A2' THEN 'ค้างชำระ 1 วัน'
      WHEN rsm.LOAN_CLASS_STR = 'A3' THEN 'ค้างชำระ 31 วัน'
      WHEN rsm.LOAN_CLASS_STR = 'A4' THEN 'ค้างชำระ 91 วัน'
      WHEN rsm.LOAN_CLASS_STR = 'A5' THEN 'ค้างชำระ 121 วัน'
      WHEN rsm.LOAN_CLASS_STR = 'A6' THEN 'ค้างชำระ 151 วัน'
      WHEN rsm.LOAN_CLASS_STR = 'A7' THEN 'ค้างชำระ 181 วัน'
      WHEN rsm.LOAN_CLASS_STR = 'A8' THEN 'ค้างชำระ 361 วัน'
      ELSE 'ไม่ทราบสถานะ'
    END AS LOAN_CLASSIFICATION,

    rsm.GROUP_FLAG,
    rsm.END_CONTRACT_DATE,

    pr.HA_NO, pr.HA_BUILDING, pr.HA_FLOOR, pr.HA_ROOMNO,
    pr.HA_VILLAGE, pr.HA_MOO, pr.HA_TRONG, pr.HA_SOI, pr.HA_ROAD,
    pr.HA_TAMNAME, pr.HA_AMPNAME, pr.HA_PROVNAME, pr.HA_MOICODE, pr.HA_POSTAL_CODE,

    la.TOTAL_AMT,

    /* ภาระหนี้คงเหลือ (ยอดปิดบัญชี) */
    CASE
      WHEN NVL(fr.RESTRUCTURE_FLAG, 'N') <> 'Y' THEN
        (NVL(t1.CAPITAL_REMAIN, 0) + NVL(t1.ACCRUED_INTEREST, 0) + NVL(t1.ACCRUED_FINE, 0) + NVL(tid.INTEREST, 0) + NVL(tid.FINE, 0))
      WHEN fr.RESTRUCTURE_FLAG = 'Y' THEN
        (NVL(t2.CAPITAL_REMAIN, 0) + NVL(t2.ACCRUED_INTEREST, 0) + NVL(t2.CARRY_INTEREST, 0) + NVL(tid.INTEREST, 0))
    END AS "ภาระหนี้รวม",

    ktb.O_BS_CAPITAL_REMAIN AS "เงินต้นคงเหลือ",
    (ktb.O_BS_ACCRUED_INTEREST + tid.INTEREST) AS "ดอกเบี้ย",
    (ktb.O_BS_ACCRUED_FINE + tid.FINE) AS "เบี้ยปรับ",
    t2.CARRY_INTEREST_CAL AS "ดอกเบี้ยพักแขวน",
    t2.CARRY_FINE_CAL AS "เบี้ยปรับพักแขวน",

    /* ยอดหนี้ค้างชำระ (รวม) */
    CASE
  WHEN fr.RESTRUCTURE_FLAG = 'Y' THEN
    ( NVL(t2.ACCRUED_INSTALLMENT, 0)
    + NVL(t2.ACCRUED_INTEREST, 0)
    + NVL(t2.ACCRUED_FINE, 0)
    + CASE WHEN bv.pick_version = 2 THEN NVL(tid.INTEREST, 0) ELSE 0 END
    + CASE WHEN bv.pick_version = 2 THEN NVL(tid.FINE, 0) ELSE 0 END
    )
  ELSE
    ( NVL(t1.ACCRUED_INSTALLMENT, 0)
    + NVL(t1.ACCRUED_INTEREST, 0)
    + NVL(t1.ACCRUED_FINE, 0)
    + CASE WHEN bv.pick_version = 1 THEN NVL(tid.INTEREST, 0) ELSE 0 END
    + CASE WHEN bv.pick_version = 1 THEN NVL(tid.FINE, 0) ELSE 0 END
    )
END AS "ยอดหนี้ค้างชำระ (ยอดผิดนัดชำระหนี้)",
    
    NVL(t1.ACCRUED_INSTALLMENT, 0) AS "ยอดหนี้เงินต้นค้างชำระ (V1)",
    NVL(bv1.ACCRUED_INTEREST, 0) AS "ดอกเบี้ยเงินต้นค้างชำระ (V1)",
    NVL(bv1.ACCRUED_FINE, 0) AS "เบี้ยปรับเงินต้นค้างชำระ (V1)",

    NVL(t2.ACCRUED_INSTALLMENT, 0) AS "ยอดหนี้เงินต้นค้างชำระ (V2)",
    NVL(bv2.ACCRUED_INTEREST, 0) AS "ดอกเบี้ยเงินต้นค้างชำระ (V2)",
    NVL(bv2.ACCRUED_FINE, 0) AS "เบี้ยปรับเงินต้นค้างชำระ (V2)",

    ktb.O_BS_LAWYER_FEE AS "ค่าธรรมเนียม",
    ar.ORG AS "หักองค์กรนายจ้าง",
    lc.max_cal_date AS "ข้อมูล ณ",

    ROW_NUMBER() OVER (PARTITION BY bc.ACC_NO ORDER BY bc.ACC_NO) AS rn

  FROM scope_list bc
  LEFT JOIN rsm_safe rsm         ON bc.ACC_NO = rsm.ACC_NO
  LEFT JOIN bill_ver bv          ON bc.ACC_NO = bv.account_no
  LEFT JOIN tbs_v1 t1            ON bc.ACC_NO = t1.account_no
  LEFT JOIN tbs_v2 t2            ON bc.ACC_NO = t2.account_no
  LEFT JOIN bills_v1 bv1 ON bc.ACC_NO = bv1.account_no
  LEFT JOIN bills_v2 bv2 ON bc.ACC_NO = bv2.account_no
  LEFT JOIN contract_pick c      ON rsm.CID   = c.CID
  LEFT JOIN DMSDBA.RDBBANK rb    ON c.STU_BANK_CODE = rb.BANK_CODE
  LEFT JOIN DMS.MV_DMS_ACC_REPORT ar ON bc.ACC_NO = ar.ACC_NO
  LEFT JOIN DMSDBA.PERSON pr     ON rsm.CIF   = pr.CIF
  LEFT JOIN DMSDBA.LOAN_ACCOUNT la   ON bc.ACC_NO = la.ACC_NO
  LEFT JOIN DMS.DMS_TRN_INPUT_FOR_RECAL fr ON bc.ACC_NO = fr.ACCOUNT_NO
  LEFT JOIN tid_pick tid         ON bc.ACC_NO = tid.account_no
  LEFT JOIN DMS.MV_KTB_DAY_12_02_2026 ktb ON bc.ACC_NO = ktb.ACC_NO
  LEFT JOIN log_cal_max lc       ON bc.ACC_NO = lc.account_no
)

SELECT /*+ PARALLEL(m 64) */
  "เลขที่บัญชี",
  "เลขประจำตัวประชาชน",
  HR_NAME || ' ' || HR_SURNAME AS FULL_NAME,
  "เงินต้นคงเหลือ",
  "ดอกเบี้ย",
  "เบี้ยปรับ",
  "ดอกเบี้ยพักแขวน",
  "เบี้ยปรับพักแขวน",
  "ภาระหนี้รวม",
  "ยอดหนี้เงินต้นค้างชำระ (V1)",
  "ดอกเบี้ยเงินต้นค้างชำระ (V1)",
  "เบี้ยปรับเงินต้นค้างชำระ (V1)",
  "ยอดหนี้เงินต้นค้างชำระ (V2)",
  "ดอกเบี้ยเงินต้นค้างชำระ (V2)",
  "เบี้ยปรับเงินต้นค้างชำระ (V2)",
  "ยอดหนี้ค้างชำระ (ยอดผิดนัดชำระหนี้)"
FROM main_data m
WHERE rn = 1
order by "เลขที่บัญชี";


