-- ALTER SESSION ENABLE PARALLEL QUERY;
-- ALTER SESSION ENABLE PARALLEL DML;

WITH
/* =========================================================
 CTE 0: Scope List
 ========================================================= */
scope_list AS (
  SELECT /*+ MATERIALIZE PARALLEL(64) */
    ACC_NO,
    CID,
    CIF
  FROM PGETMP.LIST_ACC_BROKEN_CONTRACT
  WHERE ACC_NO = '1000047458'
),

/* =========================================================
 1) bill_ver: หา Version ล่าสุด
 ========================================================= */
bill_ver AS (
  SELECT /*+ MATERIALIZE PARALLEL(b 64) */
    b.account_no,
    MAX(b.version) AS pick_version
  FROM DMS.DMS_TRN_BILL_SUMMARY b
  WHERE EXISTS (
      SELECT 1 FROM scope_list s WHERE s.ACC_NO = b.account_no
  )
  GROUP BY b.account_no
),

/* =========================================================
 2) contract_pick
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
      WHERE EXISTS (
          SELECT 1 FROM scope_list s WHERE s.CID = c.cid
      )
    )
  WHERE rn = 1
),

/* =========================================================
 3) rsm_safe
 ========================================================= */
rsm_safe AS (
  SELECT /*+ MATERIALIZE PARALLEL(64) */
    ACC_NO,
    LOAN_TYPE,
    STA_CODE,
    LOAN_ACC_STATUS,
    GROUP_FLAG,
    END_CONTRACT_DATE,
    NO_OF_LATE_BILL,
    LAST_PAYMENT_DATE,
    ENTITY,
    LOAN_CLASSIFICATION as LOAN_CLASS_STR
  FROM DMSDBA.REPORT_SUMMARY_MONTH
  WHERE EXISTS (
      SELECT 1 FROM scope_list s WHERE s.ACC_NO = ACC_NO
  )
),

/* =========================================================
 4) log_cal_max
 ========================================================= */
log_cal_max AS (
  SELECT /*+ MATERIALIZE PARALLEL(lc 64) */
    lc.account_no,
    MAX(lc.cal_date) AS max_cal_date
  FROM DMS.DMS_LOG_CAL lc
  WHERE EXISTS (
      SELECT 1 FROM scope_list s WHERE s.ACC_NO = lc.account_no
  )
  GROUP BY lc.account_no
),

/* =========================================================
 5) main_data
 ========================================================= */
main_data AS (
  SELECT /*+ QB_NAME(main) LEADING(bc) USE_HASH(bc ar c rb rsm pr la fr tbs1 tbs2 tid ktb lc bv) PARALLEL(bc 64) */
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
    bc.CIF AS "รหัสผู้กู้",
    bc.CID AS "เลขประจำตัวประชาชน",
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
    pr.HA_NO,
    pr.HA_BUILDING,
    pr.HA_FLOOR,
    pr.HA_ROOMNO,
    pr.HA_VILLAGE,
    pr.HA_MOO,
    pr.HA_TRONG,
    pr.HA_SOI,
    pr.HA_ROAD,
    pr.HA_TAMNAME,
    pr.HA_AMPNAME,
    pr.HA_PROVNAME,
    pr.HA_MOICODE,
    pr.HA_POSTAL_CODE,
    la.TOTAL_AMT,

    -- ภาระหนี้คงเหลือ (ยอดปิดบัญชี)
    CASE
      WHEN NVL(fr.RESTRUCTURE_FLAG, 'N') <> 'Y' THEN
        (NVL(tbs1.CAPITAL_REMAIN, 0) + NVL(tbs1.ACCRUED_INTEREST, 0) + NVL(tbs1.ACCRUED_FINE, 0) + NVL(tid.INTEREST, 0) + NVL(tid.FINE, 0))
      WHEN fr.RESTRUCTURE_FLAG = 'Y' THEN
        (NVL(tbs2.CAPITAL_REMAIN, 0) + NVL(tbs2.ACCRUED_INTEREST, 0) + NVL(tbs2.CARRY_INTEREST, 0) + NVL(tid.INTEREST, 0))
    END AS "ภาระหนี้คงเหลือ (ยอดปิดบัญชี)",

    ktb.O_BS_CAPITAL_REMAIN AS "เงินต้นคงเหลือ",
    ktb.O_BS_ACCRUED_INTEREST AS "ดอกเบี้ย",
    ktb.O_BS_ACCRUED_FINE AS "เบี้ยปรับ",

    -- ยอดหนี้ค้างชำระ (รวม)
    (NVL(tbs1.ACCRUED_INSTALLMENT, 0) + NVL(tbs2.ACCRUED_INSTALLMENT, 0) +
     NVL(tbs1.ACCRUED_INTEREST, 0) + NVL(tbs2.ACCRUED_INTEREST, 0) +
     NVL(tbs1.ACCRUED_FINE, 0) + NVL(tbs2.ACCRUED_FINE, 0)) AS "ยอดหนี้ค้างชำระ (ยอดผิดนัดชำระหนี้)",

    -- [NEW] แยกยอดหนี้เงินต้นค้างชำระ V1
    NVL(tbs1.ACCRUED_INSTALLMENT, 0) AS "ยอดหนี้เงินต้นค้างชำระ (V1)",
    --ดอกเบี้ยเงินต้นค้างชำระ V1
        (NVL(tbs1.ACCRUED_INTEREST, 0) +
     CASE WHEN bv.pick_version = 1 THEN NVL(tid.INTEREST, 0) ELSE 0 END
    ) AS "ดอกเบี้ยเงินต้นค้างชำระ (V1)",
      --เบี้ยปรับเงินต้นค้างชำระ V1
          CASE
      WHEN fr.RESTRUCTURE_FLAG = 'Y' THEN NVL(tbs2.CARRY_FINE, 0)
      ELSE (NVL(tbs1.ACCRUED_FINE, 0) +
            CASE WHEN bv.pick_version = 1 THEN NVL(tid.FINE, 0) ELSE 0 END)
    END AS "เบี้ยปรับเงินต้นค้างชำระ (V1)",
    -- แยกยอดหนี้เงินต้นค้างชำระ V2
    NVL(tbs2.ACCRUED_INSTALLMENT, 0) AS "ยอดหนี้เงินต้นค้างชำระ (V2)",

    -- ดอกเบี้ยเงินต้นค้างชำระ V2
    (NVL(tbs2.ACCRUED_INTEREST, 0) +
     CASE WHEN bv.pick_version = 2 THEN NVL(tid.INTEREST, 0) ELSE 0 END
    ) AS "ดอกเบี้ยเงินต้นค้างชำระ (V2)",

    -- เบี้ยปรับเงินต้นค้างชำระ V2
    (NVL(tbs2.ACCRUED_FINE, 0) +
     CASE WHEN bv.pick_version = 2 THEN NVL(tid.FINE, 0) ELSE 0 END
    ) AS "เบี้ยปรับเงินต้นค้างชำระ (V2)",

    ktb.O_BS_LAWYER_FEE AS "ค่าธรรมเนียม",
    ar.ORG AS "หักองค์กรนายจ้าง",
    lc.max_cal_date AS "ข้อมูล ณ",
    ROW_NUMBER() OVER (PARTITION BY bc.ACC_NO ORDER BY bc.ACC_NO) AS rn
  FROM scope_list bc
    LEFT JOIN bill_ver bv ON bc.ACC_NO = bv.account_no
    LEFT JOIN DMS.DMS_TRN_BILL_SUMMARY tbs1 ON bc.ACC_NO = tbs1.ACCOUNT_NO AND tbs1.VERSION = 1
    LEFT JOIN DMS.DMS_TRN_BILL_SUMMARY tbs2 ON bc.ACC_NO = tbs2.ACCOUNT_NO AND tbs2.VERSION = 2
    LEFT JOIN contract_pick c ON bc.CID = c.CID
    LEFT JOIN DMSDBA.RDBBANK rb ON c.STU_BANK_CODE = rb.BANK_CODE
    LEFT JOIN DMS.MV_DMS_ACC_REPORT ar ON bc.ACC_NO = ar.ACC_NO
    LEFT JOIN rsm_safe rsm ON bc.ACC_NO = rsm.ACC_NO
    LEFT JOIN DMSDBA.PERSON pr ON bc.CIF = pr.CIF
    LEFT JOIN DMSDBA.LOAN_ACCOUNT la ON bc.ACC_NO = la.ACC_NO
    LEFT JOIN DMS.DMS_TRN_INPUT_FOR_RECAL fr ON bc.ACC_NO = fr.ACCOUNT_NO
    LEFT JOIN DMS.DMS_TRN_CAL_INT_ADD_DAY tid ON bc.ACC_NO = tid.ACCOUNT_NO
    LEFT JOIN DMS.MV_KTB_DAY_24_01_2026 ktb ON bc.ACC_NO = ktb.ACC_NO
    LEFT JOIN log_cal_max lc ON bc.ACC_NO = lc.account_no
)

/* =========================================================
 Final SELECT
 ========================================================= */
SELECT /*+ PARALLEL(m 64) */
  "ดูแลบัญชีโดย",
  "เลขที่บัญชี",
  "ประเภทบัญชี",
  "รหัสสถานะบัญชี",
  "สถานะบุคคล",
  "สถานะบัญชี",
  "รหัสผู้กู้",
  "เลขประจำตัวประชาชน",
  TITLE_NAME,
  HR_NAME,
  HR_SURNAME,
  LAST_PAYMENT_DATE,
  NO_OF_LATE_BILL,
  LOAN_CLASSIFICATION,
  GROUP_FLAG,
  END_CONTRACT_DATE,
  HA_NO,
  HA_BUILDING,
  HA_FLOOR,
  HA_ROOMNO,
  HA_VILLAGE,
  HA_MOO,
  HA_TRONG,
  HA_SOI,
  HA_ROAD,
  HA_TAMNAME,
  HA_AMPNAME,
  HA_PROVNAME,
  HA_MOICODE,
  TOTAL_AMT,
  "ภาระหนี้คงเหลือ (ยอดปิดบัญชี)",
  "เงินต้นคงเหลือ",
  "ดอกเบี้ย",
  "เบี้ยปรับ",
  "ยอดหนี้ค้างชำระ (ยอดผิดนัดชำระหนี้)",

  -- [NEW] แสดงยอดแยก V1/V2
  "ยอดหนี้เงินต้นค้างชำระ (V1)",
    "ดอกเบี้ยเงินต้นค้างชำระ (V1)",
      "เบี้ยปรับเงินต้นค้างชำระ (V1)",

  "ยอดหนี้เงินต้นค้างชำระ (V2)",
  "ดอกเบี้ยเงินต้นค้างชำระ (V2)",
  "เบี้ยปรับเงินต้นค้างชำระ (V2)",
  "ค่าธรรมเนียม",
  "หักองค์กรนายจ้าง",
  "ข้อมูล ณ"
FROM main_data m
WHERE rn = 1;