WITH data_rank AS (
    SELECT a.cid,
        a.ACC_NO,
        a.GROUP_FLAG,
        b.RESTRUCTURE_FLAG,
        -- 1. Logic Total Amount
        CASE
            WHEN b.RESTRUCTURE_FLAG = 'Y'
            AND d.ACCOUNT_NO IS NOT NULL THEN NVL(d.PERIOD_INSTALLMENT_AMOUNT, 0)
            ELSE 0
        END AS "จำนวนเงินที่ต้องชำระต่อเดือน",
        a.NO_OF_LATE_BILL AS "จำนวนวันที่ผิดนัดชำระ",
        -- 2. NEW LOGIC: นับเฉพาะงวดที่ 'FINE_CAL_AMOUNT' > 0 (ยอดค่าปรับคงเหลือจริง)
        -- วิธีนี้จะนับได้ 3 ตาม CSV เป๊ะๆ เพราะงวดที่จ่ายแล้ว FINE_CAL_AMOUNT จะเป็น 0
        COUNT(
            CASE
                WHEN NVL(c.FINE_CAL_AMOUNT, 0) > 0 THEN 1
            END
        ) OVER (PARTITION BY a.ACC_NO) AS "จำนวนงวดที่ผิดนัดชำระ",
        -- 3. Ranking
        ROW_NUMBER() OVER (
            PARTITION BY a.ACC_NO
            ORDER BY CASE
                    WHEN b.RESTRUCTURE_FLAG = 'Y'
                    AND d.ACCOUNT_NO IS NOT NULL THEN 1
                    ELSE 2
                END ASC,
                CASE
                    WHEN b.RESTRUCTURE_FLAG = 'Y'
                    AND d.ACCOUNT_NO IS NOT NULL THEN NVL(d.PERIOD_INSTALLMENT_AMOUNT, 0)
                    ELSE 0
                END DESC
        ) as rn
    FROM DMSDBA.REPORT_SUMMARY_MONTH a
        JOIN DMS.DMS_TRN_INPUT_FOR_RECAL b ON a.ACC_NO = b.ACCOUNT_NO
        LEFT JOIN DMS.DMS_TRN_BILLS c ON b.ACCOUNT_NO = c.ACCOUNT_NO
        LEFT JOIN DMS.DMS_MST_INSTALLMENTS d ON b.ACCOUNT_NO = d.ACCOUNT_NO
        AND d.VERSION = 2
    WHERE a.ACC_NO IN ('1005321973', '1003455654')
)
SELECT DISTINCT cid,
    ACC_NO,
    GROUP_FLAG,
    RESTRUCTURE_FLAG,
    'จำนวนเงินที่ต้องชำระต่อเดือน',
    'จำนวนวันที่ผิดนัดชำระ',
    'จำนวนงวดที่ผิดนัดชำระ'
FROM data_rank
WHERE rn = 1;
WITH -- 1. หาจำนวนงวดที่ผิดนัดชำระ (นับแยกต่างหากเพื่อไม่ให้ไปคูณกับตารางอื่น)
bill_stats AS (
    SELECT ACCOUNT_NO,
        COUNT(
            CASE
                WHEN NVL(FINE_CAL_AMOUNT, 0) > 0 THEN 1
            END
        ) AS cnt_default
    FROM DMS.DMS_TRN_BILLS
    GROUP BY ACCOUNT_NO
),
-- 2. หายอดผ่อนชำระจากตาราง Installment (ดึงมาแค่บรรทัดเดียวต่อ Account)
inst_plan AS (
    SELECT DISTINCT ACCOUNT_NO,
        FIRST_VALUE(PERIOD_INSTALLMENT_AMOUNT) OVER (
            PARTITION BY ACCOUNT_NO
            ORDER BY PERIODS DESC
        ) as installment_amt
    FROM DMS.DMS_MST_INSTALLMENTS
    WHERE VERSION = 2
),
bill_ver AS (
    SELECT b.account_no,
        CASE
            WHEN MAX(
                CASE
                    WHEN b.version = 2 THEN 1
                    ELSE 0
                END
            ) = 1 THEN 2
            ELSE 1
        END AS pick_version
    FROM DMS.DMS_TRN_BILLS b
    GROUP BY b.account_no
),
bills_summary AS (
    SELECT b.account_no,
        SUM(NVL(b.installment_cal_amount, 0)) AS sum_installment,
        SUM(NVL(b.interest_cal_amount, 0)) AS sum_interest,
        SUM(NVL(b.fine_cal_amount, 0)) AS sum_fine
    FROM DMS.DMS_TRN_BILLS b
        JOIN bill_ver bv ON bv.account_no = b.account_no
    WHERE b.status = '1'
        AND b.closed_date IS NULL
        AND b.version = bv.pick_version
    GROUP BY b.account_no
),
-- 3. รวมข้อมูลทั้งหมด
data_rank AS (
    SELECT a.cid,
        a.ACC_NO,
        a.GROUP_FLAG,
        b.RESTRUCTURE_FLAG,
        -- Logic Total Amount
        CASE
            WHEN b.RESTRUCTURE_FLAG = 'Y'
            AND d.ACCOUNT_NO IS NOT NULL THEN NVL(d.installment_amt, 0)
            ELSE 0
        END AS total_amount,
        a.NO_OF_LATE_BILL,
        -- Logic Count Default (ดึงจาก CTE ที่คำนวณไว้แล้ว)
        NVL(c.cnt_default, 0) AS cnt_default,
        e.CAPITAL_REMAIN,
        NVL(tb.sum_installment, 0) AS "ยอดหนี้เงินต้นค้างชำระ",
        -- Ranking
        ROW_NUMBER() OVER (
            PARTITION BY a.ACC_NO
            ORDER BY CASE
                    WHEN b.RESTRUCTURE_FLAG = 'Y'
                    AND d.ACCOUNT_NO IS NOT NULL THEN 1
                    ELSE 2
                END ASC,
                CASE
                    WHEN b.RESTRUCTURE_FLAG = 'Y'
                    AND d.ACCOUNT_NO IS NOT NULL THEN NVL(d.installment_amt, 0)
                    ELSE 0
                END DESC
        ) as rn
    FROM DMSDBA.REPORT_SUMMARY_MONTH a
        JOIN DMS.DMS_TRN_INPUT_FOR_RECAL b ON a.ACC_NO = b.ACCOUNT_NO -- Join กับ CTE ที่เตรียมไว้ (1:1 Relation) รับรองว่าเลขไม่เบิ้ลแน่นอน
        LEFT JOIN bill_stats c ON b.ACCOUNT_NO = c.ACCOUNT_NO
        LEFT JOIN inst_plan d ON b.ACCOUNT_NO = d.ACCOUNT_NO
        LEFT JOIN DMS.DMS_TRN_BILL_SUMMARY e ON b.ACCOUNT_NO = e.ACCOUNT_NO
        LEFT JOIN bills_summary tb ON a.ACC_NO = tb.ACCOUNT_NO
    WHERE a.ACC_NO IN ('1005321973', '1003455654')
)
SELECT cid,
    ACC_NO,
    GROUP_FLAG,
    RESTRUCTURE_FLAG,
    total_amount,
    NO_OF_LATE_BILL,
    cnt_default,
    "ยอดหนี้เงินต้นค้างชำระ"
FROM data_rank
WHERE rn = 1;