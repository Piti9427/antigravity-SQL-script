-- ALTER SESSION ENABLE PARALLEL QUERY;
-- ALTER SESSION ENABLE PARALLEL DML;

WITH
/* =========================================================
 1) bill_ver: เลือก version
 ========================================================= */
bill_ver AS (
  SELECT /*+ MATERIALIZE PARALLEL(b 32) */
    b.account_no,
    CASE
      WHEN MAX(CASE WHEN NVL(b.version, 1) = 2 THEN 1 ELSE 0 END) = 1 THEN 2
      ELSE 1
    END AS pick_version
  FROM DMS.DMS_TRN_BILLS b
  WHERE b.account_no = '1056510381'
  GROUP BY b.account_no
),

/* =========================================================
 2) bills_summary: รวมยอด Bills
 ========================================================= */
bills_summary AS (
  SELECT /*+ MATERIALIZE PARALLEL(b 32) */
    b.account_no,
    SUM(NVL(b.installment_cal_amount, 0)) AS sum_installment,
    SUM(NVL(b.interest_cal_amount, 0)) AS sum_interest,
    SUM(NVL(b.fine_cal_amount, 0)) AS sum_fine
  FROM DMS.DMS_TRN_BILLS b
    JOIN bill_ver bv ON bv.account_no = b.account_no
  WHERE b.status = '1'
    AND b.closed_date IS NULL
    AND NVL(b.version, 1) = bv.pick_version
    AND b.account_no = '1056510381'
    AND b.DUE_PERIOD < TRUNC(SYSDATE)
  GROUP BY b.account_no
),

/* =========================================================
 3) contract_pick: [FIXED] เลือกเฉพาะคอลัมน์ที่ใช้ (เลี่ยง CLOB)
 ========================================================= */
contract_pick AS (
  SELECT /*+ MATERIALIZE PARALLEL(32) */
    cid,             -- เลือกมาแค่นี้พอ
    stu_bank_code    -- เลือกมาแค่นี้พอ
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
    )
  WHERE rn = 1
),

/* =========================================================
 4) main_data: Query หลัก
 ========================================================= */
main_data AS (
  SELECT
    /*+
       QB_NAME(main)
       LEADING(bc)
       USE_HASH(ar c rb rsm la fr tbs tid ktb tb)
       PARALLEL(bc 32)
       PARALLEL(ar 16)
       PARALLEL(rsm 16)
       PARALLEL(la 16)
       PARALLEL(fr 16)
       PARALLEL(tbs 32)
       PARALLEL(tid 16)
       PARALLEL(ktb 16)
       PARALLEL(tb 32)
    */
    NVL(rb.BANK_NAME_TH, '(ไม่พบธนาคาร)') AS "ดูแลบัญชีโดย",
    bc.ACC_NO AS "เลขที่บัญชี",
    ar.LOAN_TYPE AS "ประเภทบัญชี",
    ar.LOAN_ACC_STATUS AS "รหัสสถานะบัญชี",
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
    ar.TITLE_NAME,
    ar.HR_NAME,
    ar.HR_SURNAME,
    la.LAST_PAYMENT_DATE,
    ar.NO_OF_LATE_BILL,
    
    -- [Check Point] แก้ CASE เผื่อ LOAN_CLASSIFICATION เป็น Type แปลกๆ
    CASE
      WHEN TO_CHAR(rsm.LOAN_CLASSIFICATION) = 'A1' THEN 'ปกติ'
      WHEN TO_CHAR(rsm.LOAN_CLASSIFICATION) = 'A2' THEN 'ค้างชำระ 1 วัน'
      WHEN TO_CHAR(rsm.LOAN_CLASSIFICATION) = 'A3' THEN 'ค้างชำระ 31 วัน'
      WHEN TO_CHAR(rsm.LOAN_CLASSIFICATION) = 'A4' THEN 'ค้างชำระ 91 วัน'
      WHEN TO_CHAR(rsm.LOAN_CLASSIFICATION) = 'A5' THEN 'ค้างชำระ 121 วัน'
      WHEN TO_CHAR(rsm.LOAN_CLASSIFICATION) = 'A6' THEN 'ค้างชำระ 151 วัน'
      WHEN TO_CHAR(rsm.LOAN_CLASSIFICATION) = 'A7' THEN 'ค้างชำระ 181 วัน'
      WHEN TO_CHAR(rsm.LOAN_CLASSIFICATION) = 'A8' THEN 'ค้างชำระ 361 วัน'
      ELSE 'ไม่ทราบสถานะ'
    END AS LOAN_CLASSIFICATION,
    
    rsm.GROUP_FLAG,
    rsm.END_CONTRACT_DATE,
    ar.HA_NO,
    ar.HA_BUILDING,
    ar.HA_FLOOR,
    ar.HA_ROOMNO,
    ar.HA_VILLAGE,
    ar.HA_MOO,
    ar.HA_TRONG,
    ar.HA_SOI,
    ar.HA_ROAD,
    ar.HA_TAMNAME,
    ar.HA_AMPNAME,
    ar.HA_PROVNAME,
    ar.HA_MOICODE,
    ar.HA_POSTAL_CODE,

    la.TOTAL_AMT,

    CASE
      WHEN NVL(fr.RESTRUCTURE_FLAG, 'N') <> 'Y' THEN NVL(tbs.CAPITAL_REMAIN, 0) + NVL(tbs.ACCRUED_INTEREST, 0) + NVL(tbs.ACCRUED_FINE, 0) + NVL(tid.INTEREST, 0) + NVL(tid.FINE, 0)
      WHEN fr.RESTRUCTURE_FLAG = 'Y' THEN NVL(tbs.CAPITAL_REMAIN, 0) + NVL(tbs.ACCRUED_INTEREST, 0) + NVL(tbs.CARRY_INTEREST, 0) + NVL(tid.INTEREST, 0)
    END AS "ภาระหนี้คงเหลือ (ยอดปิดบัญชี)",

    ktb.O_BS_CAPITAL_REMAIN AS "เงินต้นคงเหลือ",
    ktb.O_BS_ACCRUED_INTEREST AS "ดอกเบี้ย",
    ktb.O_BS_ACCRUED_FINE AS "เบี้ยปรับ",

    (NVL(tb.sum_installment, 0) + NVL(tb.sum_interest, 0) + NVL(tb.sum_fine, 0)) AS "ยอดหนี้ค้างชำระ (ยอดผิดนัดชำระหนี้)",
    NVL(tb.sum_installment, 0) AS "ยอดหนี้เงินต้นค้างชำระ",
    (NVL(tb.sum_interest, 0) + NVL(tid.INTEREST, 0)) AS "ดอกเบี้ยเงินต้นค้างชำระ",
    
    CASE
      WHEN fr.RESTRUCTURE_FLAG = 'Y' THEN (NVL(tbs.ACCRUED_FINE, 0) + NVL(tid.FINE, 0))
      ELSE (NVL(tb.sum_fine, 0) + NVL(tid.FINE, 0))
    END AS "เบี้ยปรับเงินต้นค้างชำระ",

    ktb.O_BS_LAWYER_FEE AS "ค่าธรรมเนียม",
    ar.ORG AS "หักองค์กรนายจ้าง",
    sysdate AS "ข้อมูล ณ",
    ROW_NUMBER() OVER (PARTITION BY bc.ACC_NO ORDER BY bc.ACC_NO) AS rn
  FROM PGETMP.LIST_ACC_BROKEN_CONTRACT bc
    LEFT JOIN DMS.MV_DMS_ACC_REPORT ar ON bc.ACC_NO = ar.ACC_NO
    LEFT JOIN contract_pick c ON bc.CID = c.CID
    LEFT JOIN DMSDBA.RDBBANK rb ON c.STU_BANK_CODE = rb.BANK_CODE
    LEFT JOIN DMSDBA.REPORT_SUMMARY_MONTH rsm ON bc.ACC_NO = rsm.ACC_NO
    LEFT JOIN DMSDBA.LOAN_ACCOUNT la ON bc.ACC_NO = la.ACC_NO
    LEFT JOIN DMS.DMS_TRN_INPUT_FOR_RECAL fr ON bc.ACC_NO = fr.ACCOUNT_NO
    LEFT JOIN DMS.DMS_TRN_BILL_SUMMARY tbs ON bc.ACC_NO = tbs.ACCOUNT_NO
        AND tbs.VERSION = CASE WHEN fr.RESTRUCTURE_FLAG = 'Y' THEN 2 ELSE 1 END
    LEFT JOIN DMS.DMS_TRN_CAL_INT_ADD_DAY tid ON bc.ACC_NO = tid.ACCOUNT_NO
    LEFT JOIN DMS.MV_KTB_DAY_22_01_2026 ktb ON bc.ACC_NO = ktb.ACC_NO
    LEFT JOIN bills_summary tb ON bc.ACC_NO = tb.ACCOUNT_NO
  WHERE bc.ACC_NO = '1056510381'
)

/* =========================================================
 Final SELECT
 ========================================================= */
SELECT /*+ PARALLEL(m 32) */
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