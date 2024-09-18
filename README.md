SELECT /*+ PARALLEL(20) */
  v.act_month,
  decode(v.category, 'MINIPOS', 'Subdealer') channel,
  to_char(v.sd_code) sd_code,
  to_char(v.sd_name) sd_name,
  to_char(v.shop_name) shop_name,
  to_char(v.region) region,
  to_char(v.city) city,
  to_char(v.zone) zone,
  to_char(v.area) area,
  to_char(v.subarea) subarea,
  to_char(v.sd_subtype) sd_subtype,
  to_char(v.minipos) minipos,
  to_char(v.sd_status) sd_status,
  sum(case
        when v.sim_type = 'U-SIM' then
         1
        else
         0
      end) USIM_stock,
  sum(case
        when v.sim_type = 'Normal-SIM' then
         1
        else
         0
      end) Normal_stock,
  count(*) TotalStock,
  nvl(f.retailername, 'N/A') brandSalesRep,
  nvl(f.retailerhosname, 'N/A') BrandSuperVisor,
  round(nvl(s.sales30, 0) / 30, 2) AvgDailysales,
  s.sales30 sales30day,
  s.SALES10 sales10day,
  h.simChangeVas simChangeV,
  round(nvl(simChangeVas, 0) / 30, 2) AvgSimChage

   From stock_all_syr v
   left join brand_001_sd_info_current f
     on to_char(v.sd_code) = to_char(f.sd_code)
   left join (SELECT p.sd_code,
                     count(p.subno) sales30,
                     count(CASE
                             WHEN p.day >= trunc(sysdate - 10) THEN
                              P.subno
                           END) SALES10
                From web_custom_report_sales_temp p
               where p.day >= trunc(sysdate - 30)
               group by p.sd_code) s
     on s.sd_code = v.sd_code

   left join (SELECT u.sd_code, count(u.gsm) simChangeVas
                From sales.web_vas_temp u
               where u.day >= trunc(sysdate - 30)
                 and DESCRIPTION = 'SIM Card Change'
               group by u.sd_code) H
     on h.sd_code = v.sd_code

  group by v.act_month,
           decode(v.category, 'MINIPOS', 'Subdealer'),
           to_char(v.sd_code),
           to_char(v.sd_name),
           to_char(v.shop_name),
           to_char(v.region),
           to_char(v.city),
           to_char(v.zone),
           to_char(v.area),
           to_char(v.subarea),
           to_char(v.sd_subtype),
           to_char(v.minipos),
           to_char(v.sd_status),
           nvl(f.retailername, 'N/A'),
           nvl(f.retailerhosname, 'N/A'),
           round(nvl(s.sales30, 0) / 30, 2),
           s.sales30,
           s.SALES10,
           h.simChangeVas

  union all

SELECT /*+ PARALLEL(20) */
a.act_month,
a.sd_subtype channel,
to_char(a.sd_code) sd_code,
to_char(a.sd_name) sd_name,
to_char(a.shop_name) shop_name,
to_char(a.region) region,
to_char(a.city) city,
to_char(a.zone) zone,
to_char(a.area) area,
to_char(a.subarea) subarea,
to_char(a.sd_subtype) sd_subtype,
to_char(c.minipos) minipos,
to_char(a.sd_status) sd_status,
0 USIM_stock,
0 Normal_stock,
0 TotalStock,
a.retailername brandSalesRep,
a.retailerhosname BrandSuperVisor,
round(nvl(P.SALES30, 0) / 30, 2) AvgDailysales,
P.SALES30 sales30day,
P.SALES10 sales10day,
U.simChangeVas30 simChangeV,
round(nvl(U.simChangeVas30, 0) / 30, 2) AvgSimChage
  FROM brand_001_sd_info_current a
  left join stock_all_syr c
    on c.sd_code = a.sd_code
  left JOIN (SELECT sd_code,
                    count(p.subno) sales30,
                    COUNT(CASE
                            WHEN p.day >= trunc(sysdate - 10) THEN
                             P.subno
                          END) SALES10
               From web_custom_report_sales_temp p
              where p.day >= trunc(sysdate - 30)
              group by p.sd_code) P

    ON P.SD_CODE = A.SD_CODE

  left JOIN (SELECT u.sd_code, count(u.gsm) simChangeVas30
               From sales.web_vas_temp u
              where u.day >= trunc(sysdate - 30)
                and DESCRIPTION = 'SIM Card Change'
              group by u.sd_code) U
    ON u.sd_code = a.sd_code

WHERE to_char(a.sd_code) not in
       (SELECT to_char(b.sd_code) FROM stock_all_syr b)
   and a.sd_status = 'Active'
   and a.digitizingservice_status = 'Active'
   and a.sd_subtype like '%Sub%'
