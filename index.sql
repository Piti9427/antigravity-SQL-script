/* =========================================================
   1) bill_ver: เลือก version ของ TRN_BILLS ต่อ ACCOUNT_NO
      - ถ้ามี VERSION=2 อย่างน้อย 1 แถว -> เลือก 2
      - ไม่มีก็เลือก 1
   ========================================================= */
WITH bill_ver AS (
  SELECT
    b.account_no,
    CASE
      WHEN MAX(CASE WHEN b.version = 2 THEN 1 ELSE 0 END) = 1 THEN 2
      ELSE 1
    END AS pick_version
  FROM DMS.DMS_TRN_BILLS b
  GROUP BY b.account_no
),

/* =========================================================
   2) bills_summary: รวมยอด Bills เฉพาะบิลที่ "ยังไม่ปิด"
      - STATUS = '1'
      - CLOSED_DATE IS NULL
      - VERSION = pick_version (จาก bill_ver)
   ========================================================= */
bills_summary AS (
  SELECT
    b.account_no,
    SUM(NVL(b.installment_cal_amount,0)) AS sum_installment,
    SUM(NVL(b.interest_cal_amount,0))     AS sum_interest,
    SUM(NVL(b.fine_cal_amount,0))         AS sum_fine
  FROM DMS.DMS_TRN_BILLS b
  JOIN bill_ver bv
    ON bv.account_no = b.account_no
  WHERE b.status = '1'
    AND b.closed_date IS NULL
    AND b.version = bv.pick_version
  GROUP BY b.account_no
),

/* =========================================================
   3) contract_pick: กัน CONTRACT ซ้ำ (CID เดียวมีหลายแถว)
      - เลือกแถวที่ STU_BANK_CODE map กับ RDBBANK ได้ก่อน
      - แล้วค่อย fallback ด้วย ROWID
   ========================================================= */
contract_pick AS (
  SELECT *
  FROM (
    SELECT
      c.*,
      ROW_NUMBER() OVER (
        PARTITION BY c.cid
        ORDER BY
          CASE WHEN rb2.bank_code IS NOT NULL THEN 0 ELSE 1 END,
          CASE WHEN c.stu_bank_code IS NOT NULL THEN 0 ELSE 1 END,
          c.rowid DESC
      ) rn
    FROM DMSDBA.CONTRACT c
    LEFT JOIN DMSDBA.RDBBANK rb2
      ON c.stu_bank_code = rb2.bank_code
  )
  WHERE rn = 1
),

/* =========================================================
   4) main_data: query หลัก + กันซ้ำด้วย ROW_NUMBER
      - bills_summary เป็น LEFT JOIN (กันบัญชีหาย)
   ========================================================= */
main_data AS (
  SELECT /*+ PARALLEL(d,32) */
      NVL(rb.BANK_NAME_TH,'(ไม่พบธนาคาร)') AS "ดูแลบัญชีโดย",
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

      CASE rsm.LOAN_CLASSIFICATION
        WHEN 'A1' THEN 'ปกติ'
        WHEN 'A2' THEN 'ค้างชำระ 1 วัน'
        WHEN 'A3' THEN 'ค้างชำระ 31 วัน'
        WHEN 'A4' THEN 'ค้างชำระ 91 วัน'
        WHEN 'A5' THEN 'ค้างชำระ 121 วัน'
        WHEN 'A6' THEN 'ค้างชำระ 151 วัน'
        WHEN 'A7' THEN 'ค้างชำระ 181 วัน'
        WHEN 'A8' THEN 'ค้างชำระ 361 วัน'
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
        WHEN NVL(fr.RESTRUCTURE_FLAG,'N') <> 'Y' THEN
          NVL(tbs.CAPITAL_REMAIN,0)
          + NVL(tbs.ACCRUED_INTEREST,0)
          + NVL(tbs.ACCRUED_FINE,0)
          + NVL(tid.INTEREST,0)
          + NVL(tid.FINE,0)
        WHEN fr.RESTRUCTURE_FLAG = 'Y' THEN
          NVL(tbs.CAPITAL_REMAIN,0)
          + NVL(tbs.ACCRUED_INTEREST,0)
          + NVL(tbs.CARRY_INTEREST,0)
          + NVL(tid.INTEREST,0)
      END AS "ภาระหนี้คงเหลือ (ยอดปิดบัญชี)",

      ktb.O_BS_CAPITAL_REMAIN     AS "เงินต้นคงเหลือ",
      ktb.O_BS_ACCRUED_INTEREST   AS "ดอกเบี้ย",
      ktb.O_BS_ACCRUED_FINE       AS "เบี้ยปรับ",

      (NVL(tb.sum_installment,0) + NVL(tb.sum_interest,0) + NVL(tb.sum_fine,0)) AS "ยอดหนี้ค้างชำระ (ยอดผิดนัดชำระหนี้)",
      NVL(tb.sum_installment,0) AS "ยอดหนี้เงินต้นค้างชำระ",
      NVL(tb.sum_interest,0)    AS "ดอกเบี้ยเงินต้นค้างชำระ",
      NVL(tb.sum_fine,0)        AS "เบี้ยปรับเงินต้นค้างชำระ",

      ktb.O_BS_LAWYER_FEE         AS "ค่าธรรมเนียม",
      ar.ORG AS "หักองค์กรนายจ้าง",

      ROW_NUMBER() OVER (PARTITION BY bc.ACC_NO ORDER BY bc.ACC_NO) AS rn

  FROM PGETMP.LIST_ACC_BROKEN_CONTRACT bc
  LEFT JOIN DMS.MV_DMS_ACC_REPORT ar
    ON bc.ACC_NO = ar.ACC_NO

  LEFT JOIN contract_pick c
    ON ar.CID = c.CID

  LEFT JOIN DMSDBA.RDBBANK rb
    ON c.STU_BANK_CODE = rb.BANK_CODE

  LEFT JOIN DMSDBA.REPORT_SUMMARY_MONTH rsm
    ON ar.ACC_NO = rsm.ACC_NO

  LEFT JOIN DMSDBA.LOAN_ACCOUNT la
    ON bc.ACC_NO = la.ACC_NO

  LEFT JOIN DMS.DMS_TRN_INPUT_FOR_RECAL fr
    ON bc.ACC_NO = fr.ACCOUNT_NO

  LEFT JOIN DMS.DMS_TRN_BILL_SUMMARY tbs
    ON bc.ACC_NO = tbs.ACCOUNT_NO
    AND tbs.VERSION = CASE WHEN fr.RESTRUCTURE_FLAG = 'Y' THEN 2 ELSE 1 END

  LEFT JOIN DMS.DMS_TRN_CAL_INT_ADD_DAY tid
    ON bc.ACC_NO = tid.ACCOUNT_NO

  LEFT JOIN DMS.MV_KTB_DAY_15_01_2026 ktb
    ON bc.ACC_NO = ktb.ACC_NO

  LEFT JOIN bills_summary tb
    ON ar.ACC_NO = tb.ACCOUNT_NO

  WHERE bc.ACC_NO IN ('1000611078', '1004570732')
)

/* =========================================================
   Final SELECT: เรียงลำดับตามที่กำหนด
   ========================================================= */
SELECT
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
    HA_POSTAL_CODE,
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
    "หักองค์กรนายจ้าง"
FROM main_data
WHERE rn = 1;