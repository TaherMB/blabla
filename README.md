WITH sales_agg AS (
    SELECT 
        p.sd_code,
        COUNT(p.subno) AS sales30,
        COUNT(CASE WHEN p.day >= TRUNC(SYSDATE - 10) THEN p.subno END) AS sales10
    FROM web_custom_report_sales_temp p
    WHERE p.day >= TRUNC(SYSDATE - 30)
    GROUP BY p.sd_code
),
sim_change AS (
    SELECT 
        u.sd_code,
        COUNT(u.gsm) AS simChangeVas
    FROM sales.web_vas_temp u
    WHERE u.day >= TRUNC(SYSDATE - 30)
    AND u.description = 'SIM Card Change'
    GROUP BY u.sd_code
),
sim_change_active AS (
    SELECT 
        u.sd_code,
        COUNT(u.gsm) AS simChangeVas30
    FROM sales.web_vas_temp u
    WHERE u.day >= TRUNC(SYSDATE - 30)
    AND u.description = 'SIM Card Change'
    GROUP BY u.sd_code
)
-- Main query
SELECT /*+ PARALLEL(20) */
    v.act_month, 
    DECODE(v.category, 'MINIPOS', 'Subdealer') AS channel,
    v.sd_code,
    v.sd_name,
    v.shop_name,
    v.region,
    v.city,
    v.zone,
    v.area,
    v.subarea,
    v.sd_subtype,
    v.minipos,
    v.sd_status,
    SUM(CASE WHEN v.sim_type = 'U-SIM' THEN 1 ELSE 0 END) AS USIM_stock,
    SUM(CASE WHEN v.sim_type = 'Normal-SIM' THEN 1 ELSE 0 END) AS Normal_stock,
    COUNT(*) AS TotalStock,
    NVL(f.retailername, 'N/A') AS brandSalesRep,
    NVL(f.retailerhosname, 'N/A') AS BrandSuperVisor,
    ROUND(NVL(s.sales30, 0) / 30, 2) AS AvgDailysales,
    s.sales30 AS sales30day,
    s.sales10 AS sales10day,
    h.simChangeVas AS simChangeV,
    ROUND(NVL(h.simChangeVas, 0) / 30, 2) AS AvgSimChange
FROM stock_all_syr v
LEFT JOIN brand_001_sd_info_current f ON v.sd_code = f.sd_code
LEFT JOIN sales_agg s ON s.sd_code = v.sd_code
LEFT JOIN sim_change h ON h.sd_code = v.sd_code
GROUP BY 
    v.act_month, 
    DECODE(v.category, 'MINIPOS', 'Subdealer'),
    v.sd_code,
    v.sd_name,
    v.shop_name,
    v.region,
    v.city,
    v.zone,
    v.area,
    v.subarea,
    v.sd_subtype,
    v.minipos,
    v.sd_status,
    NVL(f.retailername, 'N/A'),
    NVL(f.retailerhosname, 'N/A'),
    ROUND(NVL(s.sales30, 0) / 30, 2),
    s.sales30,
    s.sales10,
    h.simChangeVas

UNION ALL

SELECT /*+ PARALLEL(20) */ 
    a.act_month,
    a.sd_subtype AS channel,
    a.sd_code,
    a.sd_name,
    a.shop_name,
    a.region,
    a.city,
    a.zone,
    a.area,
    a.subarea,
    a.sd_subtype,
    c.minipos,
    a.sd_status,
    0 AS USIM_stock,
    0 AS Normal_stock,
    0 AS TotalStock,
    a.retailername AS brandSalesRep,
    a.retailerhosname AS BrandSuperVisor,
    ROUND(NVL(P.sales30, 0) / 30, 2) AS AvgDailysales,
    P.sales30 AS sales30day,
    P.sales10 AS sales10day,
    U.simChangeVas30 AS simChangeV,
    ROUND(NVL(U.simChangeVas30, 0) / 30, 2) AS AvgSimChange
FROM brand_001_sd_info_current a
LEFT JOIN stock_all_syr c ON a.sd_code = c.sd_code
LEFT JOIN sales_agg P ON P.sd_code = a.sd_code
LEFT JOIN sim_change_active U ON U.sd_code = a.sd_code
WHERE a.sd_code NOT IN (SELECT b.sd_code FROM stock_all_syr b)
AND a.sd_status = 'Active'
AND a.digitizingservice_status = 'Active'
AND a.sd_subtype LIKE '%Sub%'
