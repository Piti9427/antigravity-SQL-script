-- ALTER SESSION ENABLE PARALLEL QUERY;
-- ALTER SESSION ENABLE PARALLEL DML;
-- ALTER SESSION FORCE PARALLEL QUERY PARALLEL 32;

WITH
/* =========================================================
 0) Scope List: รายชื่อบัญชีที่ต้องการ (Driver Table)
========================================================= */
scope_list AS (
  SELECT /*+ MATERIALIZE PARALLEL(64) */
    ACC_NO,
    CID
  FROM PGETMP.list_account_f69_117
  -- WHERE ACC_NO = '1000047458'
),

/* =========================================================
 1) bill_ver: ให้ "เหมือนสคริปต์เล็ก" (MAX(version) จาก BILL_SUMMARY)
========================================================= */
bill_ver AS (
  SELECT /*+ MATERIALIZE PARALLEL(b 32) */
    b.account_no,
    MAX(b.version) AS pick_version
  FROM DMS.DMS_TRN_BILL_SUMMARY b
  WHERE EXISTS (SELECT 1 FROM scope_list s WHERE s.ACC_NO = b.account_no)
  GROUP BY b.account_no
),

/* =========================================================
 2) bill_stats: นับจำนวนงวดที่ผิดนัดชำระ (Logic: ค้างจริง)
========================================================= */
bill_stats AS (
    SELECT /*+ MATERIALIZE PARALLEL(b 32) */
           b.account_no,
           COUNT(CASE
             WHEN b.STATUS = '1'
              AND b.CLOSED_DATE IS NULL
              AND b.DUE_PERIOD < TRUNC(SYSDATE)
             THEN 1
           END) AS cnt_default
    FROM DMS.DMS_TRN_BILLS b
    WHERE EXISTS (SELECT 1 FROM scope_list s WHERE s.ACC_NO = b.account_no)
    GROUP BY b.account_no
),

/* =========================================================
 3) inst_plan: หายอดผ่อนชำระต่อเดือน (V2)
========================================================= */
inst_plan AS (
    SELECT /*+ MATERIALIZE PARALLEL(i 16) */
           account_no,
           FIRST_VALUE(period_installment_amount)
             OVER (PARTITION BY account_no ORDER BY periods DESC) AS installment_amt
    FROM DMS.DMS_MST_INSTALLMENTS i
    WHERE i.version = 2
    AND EXISTS (SELECT 1 FROM scope_list s WHERE s.ACC_NO = i.account_no)
),

/* =========================================================
 4) fee_calc: คำนวณค่า fee จาก ACCOUNT_STATEMENT
========================================================= */
fee_calc AS (
    SELECT /*+ MATERIALIZE PARALLEL(s 32) */
           s.acc_no,
           ABS(SUM(CASE WHEN NVL(s.fee1, 0) < 0 THEN NVL(s.fee1, 0) ELSE 0 END)) AS fee1_new,
           ABS(SUM(CASE WHEN NVL(s.fee2, 0) < 0 THEN NVL(s.fee2, 0) ELSE 0 END)) AS fee2_new,
           ABS(SUM(CASE WHEN NVL(s.fee3, 0) < 0 THEN NVL(s.fee3, 0) ELSE 0 END)) AS fee3_new,
           ABS(SUM(CASE WHEN NVL(s.fee4, 0) < 0 THEN NVL(s.fee4, 0) ELSE 0 END)) AS fee4_new
    FROM DMSDBA.ACCOUNT_STATEMENT s
    WHERE s.tran_type IN ('4','2')
      AND EXISTS (SELECT 1 FROM scope_list sc WHERE sc.ACC_NO = s.acc_no)
      AND (
            ( s.tran_code LIKE '%ADJMON%' AND (NVL(s.remark,'') LIKE '%ค่าทนายความ%' OR NVL(s.remark,'') LIKE '%ค่าฤชา%') AND NVL(s.remark,'') NOT LIKE '%ชำระ%' )
         OR ( s.tran_code LIKE '%ADJMON%' AND (s.tran_userid LIKE '%FIX_LES%' OR s.tran_userid LIKE '%DMSFIX%') )
         OR s.tran_code IN ('DEPTER_FEE', 'ADJMONR', 'ADJLEGAL', 'REPAY', 'PAYOFF')
      )
      AND s.tran_flag = 'N' AND s.ec_revert_flag = 'N'
    GROUP BY s.acc_no
),

/* =========================================================
 5) contract_pick: กัน CONTRACT ซ้ำ
========================================================= */
contract_pick AS (
    SELECT *
    FROM (
        SELECT /*+ MATERIALIZE PARALLEL(c 8) PARALLEL(rb2 8) */
               c.*,
               ROW_NUMBER() OVER (
                   PARTITION BY c.cid
                   ORDER BY CASE WHEN rb2.bank_code IS NOT NULL THEN 0 ELSE 1 END,
                            CASE WHEN c.stu_bank_code IS NOT NULL THEN 0 ELSE 1 END,
                            c.rowid DESC
               ) rn
        FROM DMSDBA.CONTRACT c
        LEFT JOIN DMSDBA.RDBBANK rb2 ON c.stu_bank_code = rb2.bank_code
        WHERE EXISTS (SELECT 1 FROM scope_list s WHERE s.CID = c.cid)
    )
    WHERE rn = 1
),

/* =========================================================
 6) log_cal_max: ดึงวันที่ประมวลผลล่าสุด
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
 7) main_data: Query หลัก (Calculation Core)
========================================================= */
main_data AS (
    SELECT /*+
              QB_NAME(main)
              LEADING(bc)
              USE_HASH(ar c rb rsm la fr tbs1 tbs2 tid ktb bs ip fc bv lc)
              PARALLEL(bc 32)
              PARALLEL(ar 16)
              PARALLEL(rsm 16)
              PARALLEL(la 16)
              PARALLEL(fr 16)
              PARALLEL(tbs1 32)
              PARALLEL(tbs2 32)
              PARALLEL(tid 16)
              PARALLEL(ktb 16)
              PARALLEL(bs 32)
              PARALLEL(ip 16)
              PARALLEL(fc 32)
              PARALLEL(lc 16)
           */
           bc.cid AS "เลขประจำตัวประชาชน",
           bc.acc_no AS "เลขที่บัญชี",
           rsm.group_flag,
           fr.restructure_flag,

           CASE
             WHEN fr.restructure_flag = 'Y' AND ip.account_no IS NOT NULL THEN NVL(ip.installment_amt, 0)
             ELSE 0
           END AS total_amount,

           rsm.no_of_late_bill,
           NVL(bs.cnt_default, 0) AS cnt_default,

           /* ------------------------------------------------------------------
              *** ให้เหมือนสคริปต์เล็ก ***
              เงินต้นคงเหลือ/ดอกเบี้ย/เบี้ยปรับ ใช้จาก MV_KTB_DAY
           ------------------------------------------------------------------ */
           ktb.O_BS_CAPITAL_REMAIN  AS principal_remain,
           ktb.O_BS_ACCRUED_INTEREST AS interest_total,
           ktb.O_BS_ACCRUED_FINE     AS fine_total,

           /* ------------------------------------------------------------------
              V1/V2 breakdown (คง logic เดิม แต่ใช้ pick_version ใหม่จาก bill_ver)
           ------------------------------------------------------------------ */
           NVL(tbs1.ACCRUED_INSTALLMENT, 0) AS principal_v1,
           (NVL(tbs1.ACCRUED_INTEREST, 0) +
            CASE WHEN bv.pick_version = 1 THEN NVL(tid.INTEREST, 0) ELSE 0 END
           ) AS interest_v1,
           CASE
             WHEN fr.RESTRUCTURE_FLAG = 'Y' THEN NVL(tbs2.CARRY_FINE, 0)
             ELSE (NVL(tbs1.ACCRUED_FINE, 0) +
                   CASE WHEN bv.pick_version = 1 THEN NVL(tid.FINE, 0) ELSE 0 END)
           END AS fine_v1,

           NVL(tbs2.ACCRUED_INSTALLMENT, 0) AS principal_v2,
           (NVL(tbs2.ACCRUED_INTEREST, 0) +
            CASE WHEN bv.pick_version = 2 THEN NVL(tid.INTEREST, 0) ELSE 0 END
           ) AS interest_v2,
           (NVL(tbs2.ACCRUED_FINE, 0) +
            CASE WHEN bv.pick_version = 2 THEN NVL(tid.FINE, 0) ELSE 0 END
           ) AS fine_v2,

           NVL(la.fee_amt1, 0) + NVL(fc.fee1_new, 0) AS fee1_total,
           NVL(la.fee_amt2, 0) + NVL(fc.fee2_new, 0) AS fee2_total,
           NVL(la.fee_amt3, 0) + NVL(fc.fee3_new, 0) AS fee3_total,
           NVL(la.fee_amt4, 0) + NVL(fc.fee4_new, 0) AS fee4_total,

           CASE
             WHEN NVL(fr.restructure_flag, 'N') <> 'Y'
               THEN NVL(tbs1.capital_remain, 0)
                  + NVL(tbs1.accrued_interest, 0)
                  + NVL(tbs1.accrued_fine, 0)
                  + NVL(tid.interest, 0) + NVL(tid.fine, 0)
             WHEN fr.restructure_flag = 'Y'
               THEN NVL(tbs2.capital_remain, 0)
                  + NVL(tbs2.accrued_interest, 0)
                  + NVL(tbs2.carry_interest, 0)
                  + NVL(tid.interest, 0)
           END AS payoff_amount,

           lc.max_cal_date AS data_as_of,
           ROW_NUMBER() OVER (PARTITION BY bc.acc_no ORDER BY bc.acc_no) AS rn

    FROM scope_list bc
    LEFT JOIN bill_ver bv ON bc.acc_no = bv.account_no
    LEFT JOIN DMS.mv_dms_acc_report ar ON bc.acc_no = ar.acc_no
    LEFT JOIN contract_pick c ON ar.cid = c.cid
    LEFT JOIN DMSDBA.RDBBANK rb ON c.stu_bank_code = rb.bank_code
    LEFT JOIN DMSDBA.report_summary_month rsm ON ar.acc_no = rsm.acc_no
    LEFT JOIN DMSDBA.loan_account la ON bc.acc_no = la.acc_no
    LEFT JOIN DMS.dms_trn_input_for_recal fr ON bc.acc_no = fr.account_no
    LEFT JOIN DMS.dms_trn_bill_summary tbs1 ON bc.acc_no = tbs1.account_no AND tbs1.version = 1
    LEFT JOIN DMS.dms_trn_bill_summary tbs2 ON bc.acc_no = tbs2.account_no AND tbs2.version = 2
    LEFT JOIN DMS.dms_trn_cal_int_add_day tid ON bc.acc_no = tid.account_no
    LEFT JOIN DMS.mv_ktb_day_24_01_2026 ktb ON bc.acc_no = ktb.acc_no
    LEFT JOIN bill_stats bs ON bc.acc_no = bs.account_no
    LEFT JOIN inst_plan ip ON bc.acc_no = ip.account_no
    LEFT JOIN fee_calc fc ON bc.acc_no = fc.acc_no
    LEFT JOIN log_cal_max lc ON bc.acc_no = lc.account_no
)

/* =========================================================
 Final SELECT: Display Columns (ให้เหมือนสคริปต์เล็กตามชุดที่ต้องการ)
========================================================= */
SELECT /*+ PARALLEL(m 32) */
       "เลขประจำตัวประชาชน"         AS "CID",
       "เลขที่บัญชี"                 AS "ACC_NO",
       group_flag                   AS "กลุ่มผู้กู้ยืม",
       restructure_flag             AS "ปรับโครงสร้างหนี้",
       total_amount                 AS "จำนวนเงินที่ต้องชำระต่อเดือน",
       no_of_late_bill              AS "จำนวนวันที่ผิดนัดชำระ",
       cnt_default                  AS "จำนวนงวดที่ผิดนัดชำระ",

       -- ให้เหมือนสคริปต์เล็ก
       principal_remain             AS "เงินต้นคงเหลือ",
       interest_total               AS "ดอกเบี้ย",
       fine_total                   AS "เบี้ยปรับ",

       -- [NEW] แสดงยอดแยก V1/V2 (เหมือนกัน)
       principal_v1                 AS "ยอดหนี้เงินต้นค้างชำระ (V1)",
       interest_v1                  AS "ดอกเบี้ยเงินต้นค้างชำระ (V1)",
       fine_v1                      AS "เบี้ยปรับเงินต้นค้างชำระ (V1)",

       principal_v2                 AS "ยอดหนี้เงินต้นค้างชำระ (V2)",
       interest_v2                  AS "ดอกเบี้ยเงินต้นค้างชำระ (V2)",
       fine_v2                      AS "เบี้ยปรับเงินต้นค้างชำระ (V2)",

       fee1_total AS "fee 1",
       fee2_total AS "fee 2",
       fee3_total AS "fee 3",
       fee4_total AS "fee 4",
       payoff_amount                AS "ภาระหนี้คงเหลือ (ยอดปิดบัญชี)",
       data_as_of                   AS "ข้อมูล ณ"
FROM main_data m
WHERE rn = 1;