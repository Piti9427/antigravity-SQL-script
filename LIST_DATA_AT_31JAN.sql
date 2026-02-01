-- ALTER SESSION ENABLE PARALLEL QUERY;
-- ALTER SESSION ENABLE PARALLEL DML;
WITH
/* =========================================================
 CTE 0: Scope List
 ========================================================= */
scope_list AS (
    SELECT
        /*+ MATERIALIZE PARALLEL(64) */
        TRIM(ACC_NO) AS ACC_NO
    FROM PGETMP.DSL_LIST_ACCOUNT_LOT1 --   WHERE TRIM(ACC_NO) = '5000173996'
),
/* =========================================================
 Cutoff งวดปัจจุบัน (ตาม version logic เดิม)
 ========================================================= */
cutoff_v2 AS (
    -- V2: วันที่ 5 ของเดือนนี้ 00:00
    SELECT TRUNC(SYSDATE, 'MM') + 4 AS cutoff_date
    FROM dual
),
cutoff_v1 AS (
    -- V1: 5 ก.ค. ของงวดปัจจุบันตาม rule
    SELECT CASE
            WHEN SYSDATE < TO_DATE(TO_CHAR(SYSDATE, 'YYYY') || '0705', 'YYYYMMDD') THEN TO_DATE(TO_CHAR(SYSDATE, 'YYYY') || '0705', 'YYYYMMDD')
            ELSE ADD_MONTHS(
                TO_DATE(TO_CHAR(SYSDATE, 'YYYY') || '0705', 'YYYYMMDD'),
                12
            )
        END AS cutoff_date
    FROM dual
),
/* =========================================================
 1) bill_ver: หา Version ล่าสุด
 ========================================================= */
bill_ver AS (
    SELECT
        /*+ MATERIALIZE PARALLEL(b 64) */
        TRIM(b.account_no) AS account_no,
        NVL(MAX(b.version), 1) AS pick_version
    FROM DMS.DMS_TRN_BILL_SUMMARY b
    WHERE EXISTS (
            SELECT 1
            FROM scope_list s
            WHERE s.ACC_NO = TRIM(b.account_no)
        )
    GROUP BY TRIM(b.account_no)
),
/* =========================================================
 [NEW] bills_overdue: DPD จาก DMS_TRN_BILLS (STATUS=1)
 - นับจากบิลที่ "ไกลสุด" (MIN DUE_PERIOD) = DPD มาตรฐาน
 ========================================================= */
bills_overdue AS (
    SELECT
        /*+ MATERIALIZE PARALLEL(b 64) */
        TRIM(b.account_no) AS account_no,
        MIN(TRUNC(b.due_period)) AS overdue_first_due_period,
        MAX(TRUNC(b.due_period)) AS overdue_last_due_period,
        COUNT(*) AS overdue_bill_cnt,
        GREATEST(TRUNC(SYSDATE) - MIN(TRUNC(b.due_period)), 0) AS overdue_days
    FROM DMS.DMS_TRN_BILLS b
        JOIN bill_ver bv ON bv.account_no = TRIM(b.account_no)
        CROSS JOIN cutoff_v1 v1
        CROSS JOIN cutoff_v2 v2
    WHERE EXISTS (
            SELECT 1
            FROM scope_list s
            WHERE s.ACC_NO = TRIM(b.account_no)
        )
        AND TRIM(b.status) = '1'
        AND NVL(b.version, 1) = bv.pick_version
        AND TRUNC(b.due_period) < CASE
            WHEN bv.pick_version = 2 THEN v2.cutoff_date
            ELSE v1.cutoff_date
        END
    GROUP BY TRIM(b.account_no)
),
/* =========================================================
 main_data (ยึดของคุณ + เพิ่ม DPD)
 ========================================================= */
main_data AS (
    SELECT
        /*+ QB_NAME(main)
         LEADING(bc)
         USE_HASH(bc bv tbs1 tbs2 tid ktb fr bo)
         PARALLEL(64) */
        bc.ACC_NO AS "เลขที่บัญชี",
        -- ✅ เพิ่มจำนวนวันที่ค้างชำระ
        NVL(bo.overdue_days, 0) AS "จำนวนวันที่ค้างชำระ",
        bo.overdue_first_due_period AS "วันที่เริ่มค้างชำระ",
        bo.overdue_last_due_period AS "งวดค้างชำระล่าสุด",
        bo.overdue_bill_cnt AS "จำนวนงวดค้างชำระ",
        -- ====== ของเดิมที่คุณถามว่าทำไมตัด ======
        ktb.O_BS_CAPITAL_REMAIN AS "O_BS_CAPITAL_REMAIN",
        ktb.O_BS_ACCRUED_INTEREST AS "O_BS_ACCRUED_INTEREST",
        ktb.O_BS_ACCRUED_FINE AS "O_BS_ACCRUED_FINE",
        -- ยอดหนี้ค้างชำระ (รวม) ของคุณเดิม
        (
            NVL(tbs1.ACCRUED_INSTALLMENT, 0) + NVL(tbs2.ACCRUED_INSTALLMENT, 0) + NVL(tbs1.ACCRUED_INTEREST, 0) + NVL(tbs2.ACCRUED_INTEREST, 0) + NVL(tbs1.ACCRUED_FINE, 0) + NVL(tbs2.ACCRUED_FINE, 0)
        ) AS "ยอดหนี้ค้างชำระ",
        -- เบี้ยปรับที่พักไว้ ของคุณเดิม
        CASE
            WHEN fr.RESTRUCTURE_FLAG = 'Y' THEN NVL(tbs2.CARRY_FINE, 0)
            ELSE (
                NVL(tbs1.ACCRUED_FINE, 0) + CASE
                    WHEN bv.pick_version = 1 THEN NVL(tid.FINE, 0)
                    ELSE 0
                END
            )
        END AS "เบี้ยปรับที่พักไว้",
        ROW_NUMBER() OVER (
            PARTITION BY bc.ACC_NO
            ORDER BY bc.ACC_NO
        ) AS rn
    FROM scope_list bc
        LEFT JOIN bill_ver bv ON bc.ACC_NO = bv.account_no
        LEFT JOIN bills_overdue bo ON bc.ACC_NO = bo.account_no
        LEFT JOIN DMS.DMS_TRN_BILL_SUMMARY tbs1 ON bc.ACC_NO = TRIM(tbs1.ACCOUNT_NO)
        AND tbs1.VERSION = 1
        LEFT JOIN DMS.DMS_TRN_BILL_SUMMARY tbs2 ON bc.ACC_NO = TRIM(tbs2.ACCOUNT_NO)
        AND tbs2.VERSION = 2
        LEFT JOIN DMS.DMS_TRN_INPUT_FOR_RECAL fr ON bc.ACC_NO = TRIM(fr.ACCOUNT_NO)
        LEFT JOIN DMS.DMS_TRN_CAL_INT_ADD_DAY tid ON bc.ACC_NO = TRIM(tid.ACCOUNT_NO)
        LEFT JOIN DMS.MV_KTB_DAY_01_02_2026 ktb ON bc.ACC_NO = TRIM(ktb.ACC_NO)
)
/* =========================================================
 Final SELECT
 ========================================================= */
SELECT
    /*+ PARALLEL(m 64) */
    "เลขที่บัญชี",
    "ยอดหนี้ค้างชำระ",
    "เบี้ยปรับที่พักไว้",
    "O_BS_CAPITAL_REMAIN",
    "O_BS_ACCRUED_INTEREST",
    "O_BS_ACCRUED_FINE",
    "จำนวนวันที่ค้างชำระ"
FROM main_data m
WHERE rn = 1;