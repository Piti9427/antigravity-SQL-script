-- ALTER SESSION ENABLE PARALLEL QUERY;
-- ALTER SESSION ENABLE PARALLEL DML;

WITH
/* =========================================================
   CTE 0: Scope List (ดึง List บัญชีมาดักไว้ก่อน เพื่อ Performance)
   ========================================================= */
scope_list AS (
    SELECT /*+ MATERIALIZE PARALLEL(4) */ 
           ACC_NO, CID, CIF 
    FROM PGETMP.LIST_ACC_BROKEN_CONTRACT
),

/* =========================================================
   CTE 0.1: Cutoff Logic (ถอดมาจาก Store SP_BUILD_MV_KTB_DAY_PUNCH)
   ========================================================= */
cutoff_check AS (
  SELECT /*+ MATERIALIZE */
    -- V2: ใช้วันที่ปัจจุบัน (ตาม Store ที่ให้มา)
    SYSDATE AS cutoff_v2,
    
    -- V1: ตัดรอบที่ 5 ก.ค. ของปี
    CASE
       WHEN SYSDATE < TO_DATE(TO_CHAR(EXTRACT(YEAR FROM SYSDATE)) || '0705','YYYYMMDD')
       THEN TO_DATE(TO_CHAR(EXTRACT(YEAR FROM SYSDATE)) || '0705','YYYYMMDD')             -- ปีนี้
       ELSE ADD_MONTHS(TO_DATE(TO_CHAR(EXTRACT(YEAR FROM SYSDATE)) || '0705','YYYYMMDD'), 12) -- ปีหน้า
    END AS cutoff_v1
  FROM DUAL
),

/* =========================================================
 1) bill_ver: เลือก version
 ========================================================= */
bill_ver AS (
  SELECT /*+ MATERIALIZE PARALLEL(b 4) */
    TRIM(b.account_no) as account_no,
    CASE
      WHEN MAX(CASE WHEN NVL(b.version, 1) = 2 THEN 1 ELSE 0 END) = 1 THEN 2
      ELSE 1
    END AS pick_version
  FROM DMS.DMS_TRN_BILLS b
  WHERE EXISTS (SELECT 1 FROM scope_list s WHERE s.ACC_NO = TRIM(b.account_no))
  GROUP BY TRIM(b.account_no)
),

/* =========================================================
 2) bills_summary: รวมยอด Bills (ใช้ Logic Cutoff จาก Store)
 ========================================================= */
bills_summary AS (
  SELECT /*+ MATERIALIZE PARALLEL(b 4) */
    TRIM(b.account_no) as account_no,
    SUM(NVL(b.installment_cal_amount, 0)) AS sum_installment,
    SUM(NVL(b.interest_cal_amount, 0))     AS sum_interest,
    SUM(NVL(b.fine_cal_amount, 0))         AS sum_fine
  FROM DMS.DMS_TRN_BILLS b
  JOIN bill_ver bv ON TRIM(bv.account_no) = TRIM(b.account_no)
  CROSS JOIN cutoff_check c -- เอาวันที่ตัดรอบมาใช้
  WHERE b.status = '1'
    AND b.closed_date IS NULL
    AND NVL(b.version, 1) = bv.pick_version
    AND EXISTS (SELECT 1 FROM scope_list s WHERE s.ACC_NO = TRIM(b.account_no))
    
    -- [CRITICAL UPDATE] ใช้ Logic Cutoff ตาม Store Procedure
    AND (
      (bv.pick_version = 2 AND b.DUE_PERIOD <= c.cutoff_v2) -- V2: ตัดที่ปัจจุบัน
      OR
      (bv.pick_version = 1 AND b.DUE_PERIOD < c.cutoff_v1)  -- V1: ตัดที่ 5 ก.ค.
    )
    
  GROUP BY TRIM(b.account_no)
),

/* =========================================================
 3) contract_pick: เลือกเฉพาะคอลัมน์ (TRIM CID)
 ========================================================= */
contract_pick AS (
  SELECT /*+ MATERIALIZE PARALLEL(4) */
    TRIM(cid) as cid,
    TRIM(stu_bank_code) as stu_bank_code
  FROM (
      SELECT /*+ PARALLEL(c 16) PARALLEL(rb2 16) */
        c.cid,
        c.stu_bank_code,
        ROW_NUMBER() OVER (
          PARTITION BY c.cid
          ORDER BY CASE WHEN rb2.bank_code IS NOT NULL THEN 0 ELSE 1 END,
                   CASE WHEN c.stu_bank_code IS NOT NULL THEN 0 ELSE 1 END,
                   c.rowid DESC
        ) rn
      FROM DMSDBA.CONTRACT c
      LEFT JOIN DMSDBA.RDBBANK rb2 ON c.stu_bank_code = rb2.bank_code
      WHERE EXISTS (SELECT 1 FROM scope_list s WHERE TRIM(s.CID) = TRIM(c.cid))
    )
  WHERE rn = 1
),

/* =========================================================
 4) rsm_safe: ดึง RSM และแปลง CLOB
 ========================================================= */
rsm_safe AS (
  SELECT /*+ MATERIALIZE PARALLEL(4) */
    TRIM(ACC_NO) as ACC_NO,
    LOAN_TYPE,
    STA_CODE,
    LOAN_ACC_STATUS,
    GROUP_FLAG,
    END_CONTRACT_DATE,
    NO_OF_LATE_BILL,
    LAST_PAYMENT_DATE,
    ENTITY,
    DBMS_LOB.SUBSTR(LOAN_CLASSIFICATION, 100, 1) as LOAN_CLASS_STR
  FROM DMSDBA.REPORT_SUMMARY_MONTH
  WHERE EXISTS (SELECT 1 FROM scope_list s WHERE s.ACC_NO = TRIM(ACC_NO))
),

/* =========================================================
 4.1) log_cal_max: เอา MAX(CAL_DATE)
 ========================================================= */
log_cal_max AS (
  SELECT /*+ MATERIALIZE PARALLEL(lc 4) */
    TRIM(lc.account_no) AS account_no,
    MAX(lc.cal_date)    AS max_cal_date
  FROM DMS.DMS_LOG_CAL lc
  WHERE EXISTS (SELECT 1 FROM scope_list s WHERE s.ACC_NO = TRIM(lc.account_no))
  GROUP BY TRIM(lc.account_no)
),

/* =========================================================
 5) main_data: Query หลัก
 ========================================================= */
main_data AS (
  SELECT
    /*+
       QB_NAME(main)
       LEADING(bc)
       USE_HASH(bc ar c rb rsm pr la fr tbs tid ktb tb lc)
       PARALLEL(bc 4)
    */
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

    COALESCE(ar.TITLE_NAME, TO_CHAR(pr.HR_TITLE)) AS TITLE_NAME,
    COALESCE(ar.HR_NAME, pr.HR_NAME)             AS HR_NAME,
    COALESCE(ar.HR_SURNAME, pr.HR_SURNAME)       AS HR_SURNAME,

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

    CASE
      WHEN NVL(fr.RESTRUCTURE_FLAG, 'N') <> 'Y'
        THEN NVL(tbs.CAPITAL_REMAIN, 0) + NVL(tbs.ACCRUED_INTEREST, 0) + NVL(tbs.ACCRUED_FINE, 0)
           + NVL(tid.INTEREST, 0) + NVL(tid.FINE, 0)
      WHEN fr.RESTRUCTURE_FLAG = 'Y'
        THEN NVL(tbs.CAPITAL_REMAIN, 0) + NVL(tbs.ACCRUED_INTEREST, 0) + NVL(tbs.CARRY_INTEREST, 0)
           + NVL(tid.INTEREST, 0)
    END AS "ภาระหนี้คงเหลือ (ยอดปิดบัญชี)",

    ktb.O_BS_CAPITAL_REMAIN        AS "เงินต้นคงเหลือ",
    ktb.O_BS_ACCRUED_INTEREST      AS "ดอกเบี้ย",
    ktb.O_BS_ACCRUED_FINE          AS "เบี้ยปรับ",

    /* [LOGIC FROM STORE]
       - ยอดหนี้ค้างชำระ (Overdue) มาจาก CTE bills_summary ที่ใช้ Cutoff Logic แล้ว
       - ดึงแยกยอด ต้น, ดอก, ปรับ ออกมาโชว์
    */
    (NVL(tb.sum_installment, 0) + NVL(tb.sum_interest, 0) + NVL(tb.sum_fine, 0)) AS "ยอดหนี้ค้างชำระ (ยอดผิดนัดชำระหนี้)",
    NVL(tb.sum_installment, 0) AS "ยอดหนี้เงินต้นค้างชำระ",
    (NVL(tb.sum_interest, 0) + NVL(tid.INTEREST, 0)) AS "ดอกเบี้ยเงินต้นค้างชำระ",

    CASE
      WHEN fr.RESTRUCTURE_FLAG = 'Y' THEN (NVL(tbs.ACCRUED_FINE, 0) + NVL(tid.FINE, 0))
      ELSE (NVL(tb.sum_fine, 0) + NVL(tid.FINE, 0))
    END AS "เบี้ยปรับเงินต้นค้างชำระ",

    ktb.O_BS_LAWYER_FEE AS "ค่าธรรมเนียม",
    ar.ORG              AS "หักองค์กรนายจ้าง",
    lc.max_cal_date     AS "ข้อมูล ณ",

    ROW_NUMBER() OVER (PARTITION BY bc.ACC_NO ORDER BY bc.ACC_NO) AS rn

  FROM PGETMP.LIST_ACC_BROKEN_CONTRACT bc
  LEFT JOIN contract_pick c ON TRIM(bc.CID) = TRIM(c.CID)
  LEFT JOIN DMSDBA.RDBBANK rb ON c.STU_BANK_CODE = rb.BANK_CODE
  LEFT JOIN DMS.MV_DMS_ACC_REPORT ar ON TRIM(bc.ACC_NO) = TRIM(ar.ACC_NO)
  LEFT JOIN rsm_safe rsm ON TRIM(bc.ACC_NO) = TRIM(rsm.ACC_NO)
  LEFT JOIN DMSDBA.PERSON pr ON TO_NUMBER(TRIM(bc.CIF)) = TO_NUMBER(pr.CIF)
  LEFT JOIN DMSDBA.LOAN_ACCOUNT la ON TRIM(bc.ACC_NO) = TRIM(la.ACC_NO)
  LEFT JOIN DMS.DMS_TRN_INPUT_FOR_RECAL fr ON TRIM(bc.ACC_NO) = TRIM(fr.ACCOUNT_NO)
  LEFT JOIN DMS.DMS_TRN_BILL_SUMMARY tbs
    ON TRIM(bc.ACC_NO) = TRIM(tbs.ACCOUNT_NO)
   AND tbs.VERSION = CASE WHEN fr.RESTRUCTURE_FLAG = 'Y' THEN 2 ELSE 1 END
  LEFT JOIN DMS.DMS_TRN_CAL_INT_ADD_DAY tid ON TRIM(bc.ACC_NO) = TRIM(tid.ACCOUNT_NO)
  LEFT JOIN DMS.MV_KTB_DAY_22_01_2026 ktb ON TRIM(bc.ACC_NO) = TRIM(ktb.ACC_NO)
  LEFT JOIN bills_summary tb ON TRIM(bc.ACC_NO) = TRIM(tb.ACCOUNT_NO)
  LEFT JOIN log_cal_max lc ON TRIM(bc.ACC_NO) = TRIM(lc.account_no)
)

/* =========================================================
 Final SELECT
 ========================================================= */
SELECT /*+ PARALLEL(m 4) */
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
  "ยอดหนี้เงินต้นค้างชำระ",
  "ดอกเบี้ยเงินต้นค้างชำระ",
  "เบี้ยปรับเงินต้นค้างชำระ",
  "ค่าธรรมเนียม",
  "หักองค์กรนายจ้าง",
  "ข้อมูล ณ"
FROM main_data m
WHERE rn = 1;