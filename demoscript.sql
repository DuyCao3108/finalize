create table test_customer_flow_forward_052021 as 
with ini_contract as (
    SELECT
        a.*,
        row_number() over(
            partition by skp_client
            order by
                APL_SIGN_DTM asc
        ) rk
    FROM
        Vw_Tb_Crm_Appl A
    WHERE
        1 = 1
        AND a.DATE_DECISION >= date '2021-04-15'
        AND a.DATE_DECISION < date '2021-08-01'
        AND A.APL_SIGN_DTM IS NOT NULL
        and a.APL_SIGN_DTM >= date '2021-05-01'
        AND a.APL_SIGN_DTM < date '2021-08-01'
        and a.APL_CANCEL_DTM is null
        and a.apl_f_new = 1
        and a.apl_prod_code_Gr not in ('SAI') -- and id_cuid = 1343308
),
aft_ini_contract as (
    SELECT
        trunc(ini.apl_sign_dtm, 'mm') as ini_sign_month,
        ini.id_cuid as ini_id_cuid,
        ini.apl_prod_code_gr ini_contract_type,
        ini.skp_credit_Case ini_skp_credit_case,
        a.*,
        row_number() over(
            partition by a.id_cuid
            order by
                a.APL_SIGN_DTM asc
        ) rnk
    FROM
        ini_contract ini
        left join (
            SELECT
                a.APL_SIGN_DTM,
                a.id_cuid,
                a.skp_credit_Case,
                a.APL_PROD_CODE_GR
            FROM
                Vw_Tb_Crm_Appl A
            WHERE
                1 = 1
                AND a.DATE_DECISION >= date '2021-04-15'
                AND A.APL_SIGN_DTM IS NOT NULL
                and a.APL_SIGN_DTM >= date '2021-05-01'
                and a.APL_CANCEL_DTM is null
                and a.apl_prod_code_Gr not in ('SAI')
        ) a on ini.id_cuid = a.id_cuid
        and a.APL_SIGN_DTM > ini.apl_sign_dtm
    WHERE
        1 = 1
        and rk = 1
),
test_tb_customer_flow_forward as(
    select
        INI_SIGN_MONTH,
        ini_ID_CUID as id_cuid,
        ini_contract_type,
        max(
            case
                when rnk = 1 then APL_PROD_CODE_GR
            end
        ) as contract1_type,
        max(
            case
                when rnk = 2 then APL_PROD_CODE_GR
            end
        ) as contract2_type,
        max(
            case
                when rnk = 3 then APL_PROD_CODE_GR
            end
        ) as contract3_type,
        count(skp_credit_case) as contracts,
        max(
            case
                when rnk = 1 then trunc(apl_sign_dtm, 'mm')
            end
        ) as contract1_month,
        max(
            case
                when rnk = 2 then trunc(apl_sign_dtm, 'mm')
            end
        ) as contract2_month,
        max(
            case
                when rnk = 3 then trunc(apl_sign_dtm, 'mm')
            end
        ) as contract3_month
    from
        aft_ini_contract ini
    group by
        INI_SIGN_MONTH,
        ini_ID_CUID,
        ini_contract_type
),
tesst as (
    select
        ini_sign_month,
        id_cuid,
        CASE
            when ini_contract_type LIKE '%CL%' THEN 'CL'
            when INI_CONTRACT_TYPE like 'CC%'
            or INI_CONTRACT_TYPE like 'CoC%' then 'CC'
            WHEN INI_CONTRACT_TYPE IN ('BNPL') THEN 'HPL'
            ELSE ini_contract_type
        END ini_contract_type,
        CASE
            when contract1_type LIKE '%CL%' THEN 'CL'
            when contract1_type like 'CC%'
            or contract1_type like 'CoC%' then 'CC'
            WHEN contract1_type IN ('BNPL') THEN 'HPL'
            WHEN CONTRACT1_TYPE IS NULL THEN 'NC'
            ELSE contract1_type
        END contract1_type,
        CASE
            when contract2_type LIKE '%CL%' THEN 'CL'
            when contract2_type like 'CC%'
            or contract2_type like 'CoC%' then 'CC'
            WHEN contract2_type IN ('BNPL') THEN 'HPL'
            WHEN CONTRACT2_TYPE IS NULL THEN 'NC'
            ELSE contract2_type
        END contract2_type,
        CASE
            when contract3_type LIKE '%CL%' THEN 'CL'
            when contract3_type like 'CC%'
            or contract3_type like 'CoC%' then 'CC'
            WHEN contract3_type IN ('BNPL') THEN 'HPL'
            when contract3_type is null then 'NC'
            ELSE contract3_type
        END contract3_type,
        contracts,
        case
            when contracts = 0 then '0'
            when contracts = 1 then '1'
            when contracts = 2 then '2'
            when contracts = 3 then '3'
            when contracts > 3 then '3+'
        end contracts_gr,
        FLOOR(MONTHS_BETWEEN(contract1_month, ini_sign_month)) AS dur_ini_first,
        FLOOR(MONTHS_BETWEEN(contract2_month, ini_sign_month)) AS dur_ini_second,
        FLOOR(MONTHS_BETWEEN(contract3_month, ini_sign_month)) AS dur_ini_third,
        FLOOR(MONTHS_BETWEEN(contract2_month, contract1_month)) AS dur_first_second,
        FLOOR(MONTHS_BETWEEN(contract3_month, contract1_month)) AS dur_first_third,
        FLOOR(MONTHS_BETWEEN(contract3_month, contract2_month)) AS dur_second_third --  Count(*) as clients
    from
        test_tb_customer_flow_forward
        /*  group by INI_SIGN_MONTH, 
         CASE when ini_contract_type LIKE '%CL%' THEN 'CL' 
         when ini_contract_type like 'CC%' then 'CC'
         WHEN ini_contract_type IN ('BNPL') THEN 'HPL'   
         ELSE ini_contract_type
         END,-- ini_contract_type,
         CASE when contract1_type LIKE '%CL%' THEN 'CL' 
         when contract1_type like 'CC%' then 'CC'
         WHEN contract1_type IN ('BNPL') THEN 'HPL'
         WHEN CONTRACT1_TYPE IS NULL THEN 'NC'
         ELSE contract1_type
         END,-- contract1_type,
         CASE when contract2_type LIKE '%CL%' THEN 'CL' 
         when contract2_type like 'CC%' then 'CC'
         WHEN contract2_type IN ('BNPL') THEN 'HPL'
         WHEN CONTRACT2_TYPE IS NULL THEN 'NC'
         ELSE contract2_type
         END,-- contract2_type,
         CASE  when contract3_type LIKE '%CL%' THEN 'CL' 
         when contract3_type like 'CC%' then 'CC'
         WHEN contract3_type IN ('BNPL') THEN 'HPL'
         when contract3_type is null then 'NC'
         ELSE contract3_type
         END,--contract3_type,
         contracts,
         case when contracts = 0 then '0'
         when contracts = 1 then '1'
         when contracts = 2 then '2'
         when contracts = 3 then '3'
         when contracts > 3 then '3+'  
         end*/
        /*  ;
         
         
         
         with testt as(*/
)
SELECT
    ini_sign_month,
    ini_contract_type,
    contract1_type,
    contract2_type,
    contract3_type,
    contracts,
    contracts_gr,
    dur_ini_first,
    dur_ini_second,
    dur_ini_third,
    dur_first_second,
    dur_first_third,
    dur_second_third,
    count(*) as clients
FROM
    tesst
GROUP BY
    ini_sign_month,
    ini_contract_type,
    contract1_type,
    contract2_type,
    contract3_type,
    contracts,
    contracts_gr,
    dur_ini_first,
    dur_ini_second,
    dur_ini_third,
    dur_first_second,
    dur_first_third,
    dur_second_third;

select
    *
from
    test_customer_flow_forward_052021