ALTER SESSION ENABLE PARALLEL QUERY;
ALTER SESSION ENABLE PARALLEL DML;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 32;
/* =========================================================
  Session knobs (เลือกใช้ตามสิทธิ์)
  - ถ้า environment อนุญาต แนะนำเปิดไว้ก่อนรัน
========================================================= */
/*
ALTER SESSION ENABLE PARALLEL QUERY;
ALTER SESSION ENABLE PARALLEL DML;
-- ถ้าอยากบังคับให้ query วิ่ง parallel ง่ายขึ้น (ระวังแย่งเครื่อง)
-- ALTER SESSION FORCE PARALLEL QUERY PARALLEL 32;
*/

WITH
/* =========================================================
 1) bill_ver: เลือก version ที่ควรใช้ต่อ account
========================================================= */
bill_ver AS (
    SELECT /*+ MATERIALIZE PARALLEL(b 32) */
           b.account_no,
           CASE
             WHEN MAX(CASE WHEN b.version = 2 THEN 1 ELSE 0 END) = 1 THEN 2
             ELSE 1
           END AS pick_version
    FROM DMS.DMS_TRN_BILLS b
    GROUP BY b.account_no
),

/* =========================================================
 2) bills_summary: รวมยอด Bills เฉพาะบิลที่ STATUS = '1'
  - ใส่ MATERIALIZE กันโดน merge แล้ววิ่งซ้ำ
========================================================= */
bills_summary AS (
    SELECT /*+ MATERIALIZE PARALLEL(b 32) */
           b.account_no,
           SUM(NVL(b.installment_cal_amount, 0)) AS sum_installment,
           SUM(NVL(b.interest_cal_amount, 0))     AS sum_interest,
           SUM(NVL(b.fine_cal_amount, 0))         AS sum_fine
    FROM DMS.DMS_TRN_BILLS b
    WHERE b.status = '1'
    GROUP BY b.account_no
),

/* =========================================================
 3) bill_stats: นับจำนวนงวดที่ผิดนัดชำระ
========================================================= */
bill_stats AS (
    SELECT /*+ MATERIALIZE PARALLEL(b 32) */
           b.account_no,
           COUNT(CASE WHEN NVL(b.fINE_AMOUNT, 0) > 0 THEN 1 END) AS cnt_default
    FROM DMS.DMS_TRN_BILLS b
    GROUP BY b.account_no
),

/* =========================================================
 4) inst_plan: หายอดผ่อนชำระจากตาราง Installment
========================================================= */
inst_plan AS (
    SELECT /*+ MATERIALIZE PARALLEL(i 16) */
           account_no,
           FIRST_VALUE(period_installment_amount)
             OVER (PARTITION BY account_no ORDER BY periods DESC) AS installment_amt
    FROM DMS.DMS_MST_INSTALLMENTS i
    WHERE i.version = 2
),

/* =========================================================
 5) fee_calc: คำนวณค่า fee จาก ACCOUNT_STATEMENT (ตัวโหด)
  - แนะนำ degree 32/64 ตามเครื่องจริง
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
      AND (
            ( s.tran_code LIKE '%ADJMON%'
              AND (NVL(s.remark,'') LIKE '%ค่าทนายความ%' OR NVL(s.remark,'') LIKE '%ค่าฤชา%')
              AND NVL(s.remark,'') NOT LIKE '%ชำระ%'
            )
         OR ( s.tran_code LIKE '%ADJMON%'
              AND (s.tran_userid LIKE '%FIX_LES%' OR s.tran_userid LIKE '%DMSFIX%')
            )
         OR s.tran_code LIKE '%DEPTER_FEE%'
         OR s.tran_code LIKE '%ADJMONR%'
         OR s.tran_code LIKE '%ADJLEGAL%'
         OR s.tran_code LIKE '%REPAY%'
         OR s.tran_code LIKE '%PAYOFF%'
      )
      AND s.tran_flag = 'N'
      AND s.ec_revert_flag = 'N'
    GROUP BY s.acc_no
),

/* =========================================================
 6) contract_pick: กัน CONTRACT ซ้ำ (CID เดียวมีหลายแถว)
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
        LEFT JOIN DMSDBA.RDBBANK rb2
               ON c.stu_bank_code = rb2.bank_code
    )
    WHERE rn = 1
),

/* =========================================================
 7) main_data: query หลัก
  - ใช้ LEADING ให้ขับด้วย list (bc) ก่อน
  - ใช้ HASH JOIN เป็นหลัก
  - ใส่ PARALLEL ให้ “alias ที่มีอยู่จริง”
========================================================= */
main_data AS (
    SELECT /*+
              QB_NAME(main)
              LEADING(bc)
              USE_HASH(ar c rb rsm la fr tbs tid ktb tb bs ip fc)
              PARALLEL(bc 32)
              PARALLEL(ar 16)
              PARALLEL(rsm 16)
              PARALLEL(la 16)
              PARALLEL(fr 16)
              PARALLEL(tbs 32)
              PARALLEL(tid 16)
              PARALLEL(ktb 16)
              PARALLEL(tb 32)
              PARALLEL(bs 32)
              PARALLEL(ip 16)
              PARALLEL(fc 32)
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

           ktb.o_bs_capital_remain AS "เงินต้นคงเหลือ",
           NVL(tb.sum_installment, 0) AS "ยอดหนี้เงินต้นค้างชำระ",
           (NVL(tb.sum_interest, 0) + NVL(tid.interest, 0)) AS "ดอกเบี้ยเงินต้นค้างชำระ",
           CASE
                WHEN fr.restructure_flag = 'Y' THEN (NVL(tbs.ACCRUED_FINE, 0) + NVL(tid.FINE, 0))
                ELSE (NVL(tb.sum_fine, 0) + NVL(tid.FINE, 0))
            END AS "เบี้ยปรับเงินต้นค้างชำระ",

           NVL(la.fee_amt1, 0) + NVL(fc.fee1_new, 0) AS fee1_total,
           NVL(la.fee_amt2, 0) + NVL(fc.fee2_new, 0) AS fee2_total,
           NVL(la.fee_amt3, 0) + NVL(fc.fee3_new, 0) AS fee3_total,
           NVL(la.fee_amt4, 0) + NVL(fc.fee4_new, 0) AS fee4_total,

           CASE
             WHEN NVL(fr.restructure_flag, 'N') <> 'Y'
               THEN NVL(tbs.capital_remain, 0)
                  + NVL(tbs.accrued_interest, 0)
                  + NVL(tbs.accrued_fine, 0)
                  + NVL(tid.interest, 0)
                  + NVL(tid.fine, 0)
             WHEN fr.restructure_flag = 'Y'
               THEN NVL(tbs.capital_remain, 0)
                  + NVL(tbs.accrued_interest, 0)
                  + NVL(tbs.carry_interest, 0)
                  + NVL(tid.interest, 0)
           END AS "ภาระหนี้คงเหลือ (ยอดปิดบัญชี)",

           ROW_NUMBER() OVER (PARTITION BY bc.acc_no ORDER BY bc.acc_no) AS rn
    FROM PGETMP.list_account_f69_117_2 bc
    LEFT JOIN DMS.mv_dms_acc_report ar
           ON bc.acc_no = ar.acc_no
    LEFT JOIN contract_pick c
           ON ar.cid = c.cid
    LEFT JOIN DMSDBA.RDBBANK rb
           ON c.stu_bank_code = rb.bank_code
    LEFT JOIN DMSDBA.report_summary_month rsm
           ON ar.acc_no = rsm.acc_no
    LEFT JOIN DMSDBA.loan_account la
           ON bc.acc_no = la.acc_no
    LEFT JOIN DMS.dms_trn_input_for_recal fr
           ON bc.acc_no = fr.account_no
    LEFT JOIN DMS.dms_trn_bill_summary tbs
           ON bc.acc_no = tbs.account_no
          AND tbs.version = CASE WHEN fr.restructure_flag = 'Y' THEN 2 ELSE 1 END
    LEFT JOIN DMS.dms_trn_cal_int_add_day tid
           ON bc.acc_no = tid.account_no
    LEFT JOIN DMS.mv_ktb_day_21_01_2026 ktb
           ON bc.acc_no = ktb.acc_no
    LEFT JOIN bills_summary tb
           ON ar.acc_no = tb.account_no
    LEFT JOIN bill_stats bs
           ON bc.acc_no = bs.account_no
    LEFT JOIN inst_plan ip
           ON bc.acc_no = ip.account_no
    LEFT JOIN fee_calc fc
           ON bc.acc_no = fc.acc_no
)

SELECT /*+ PARALLEL(m 32) */
       "เลขประจำตัวประชาชน" AS "CID",
       "เลขที่บัญชี"         AS "ACC_NO",
       group_flag             AS "กลุ่มผู้กู้ยืม",
       restructure_flag       AS "ปรับโครงสร้างหนี้",
       total_amount           AS "จำนวนเงินที่ต้องชำระต่อเดือน",
       no_of_late_bill        AS "จำนวนวันที่ผิดนัดชำระ",
       cnt_default            AS "จำนวนงวดที่ผิดนัดชำระ",
       "เงินต้นคงเหลือ",
       "ยอดหนี้เงินต้นค้างชำระ"     AS "เงินต้นค้างชำระ",
       "ดอกเบี้ยเงินต้นค้างชำระ"    AS "ดอกเบี้ยรวม",
       "เบี้ยปรับเงินต้นค้างชำระ"   AS "เบี้ยปรับ",
       fee1_total AS "fee 1",
       fee2_total AS "fee 2",
       fee3_total AS "fee 3",
       fee4_total AS "fee 4",
       "ภาระหนี้คงเหลือ (ยอดปิดบัญชี)" AS "ยอดชำระเป็นบัญชีปกติ"
FROM main_data m
WHERE rn = 1;