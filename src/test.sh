#-GE CONFIDENTIAL- or -GE HIGHLY CONFIDENTIAL-
#Type: Source Code
#Copyright (c) 2023, GE Healthcare
#All Rights Reserved
#This unpublished material is proprietary to GE Healthcare. The methods and
#techniques described herein are considered trade secrets and/or
#confidential. Reproduction or distribution, in whole or in part, is
#forbidden except by express written permission of GE Healthcare.
# <RSQL> SCRIPT NAME - rsql_pcm_bom_di_edst_pcm_prd_instnc_s.sh </RSQL>
# <RSQL> SCRIPT DESCRIPTION - "S2F Merge Bteq Script for PCM_BOM_TEMPLATE_S"  </RSQL>
# <RSQL>  STMT TYPES    -  </RSQL>
# <RSQL>  CREATED ON    - XXX </RSQL>
# <RSQL>  CREATED BY    -   </RSQL>
# <RSQL>  LAST MODIFIED BY    - </RSQL>
# <RSQL>  LAST MODIFIED ON    - XXX  </RSQL>s

start=`date +%s`

. /ops/applications/pqw/rsql/scripts/rsql_pqw_parameter.sh 

#source /ops/common/scripts/get_redshift_creds.sh 'pqw-secret-504013648'
source /ops/common/scripts/aws_run_profile.sh $1 $2 $3 $4

echo date : `date`
echo Start Time : $(date +"%T.%N")

rsql -h $HOST -U $USER -d $DB << EOF 
\timing true

\echo '\n-----MAIN EXECUTION LOG STARTING HERE-----'


/*=============================================================================================================================
==================================================SET QUERY Group==============================================================
===============================================================================================================================*/

SET query_group to '$QBSTR';
\if :ERROR <> 0
 \echo 'Setting Query Group to $QBSTR failed '
 \echo 'Error Code -'
 \echo :ERRORCODE
 \remark :LAST_ERROR_MESSAGE
 \exit 1
\else
 \remark '\n **** Setting Query Group to $QBSTR Successfully **** \n'
\endif

/*=============================================================================================================================
====================================================Setting the database=======================================================
===============================================================================================================================*/
SET SEARCH_PATH TO hdl_pqw_etl_stage, pg_catalog;

\if :ERROR <> 0
 \echo 'Setting the database failed '
 \echo 'Error Code -'
 \echo :ERRORCODE
 \remark :LAST_ERROR_MESSAGE
 \exit 2
\else
 \remark '\n **** Setting the database Successfully **** \n'
\endif

/*==============================================================================================================================
============================================ DELETE FROM PCM_PRD_INSTNC_S ======================================================
================================================================================================================================*/
CALL hdl_pqw_sp.sp_truncate_table_pqw ('hdl_pqw_etl_stage','pcm_prd_instnc_s');


\if :ERROR <> 0
\echo 'Truncate table pcm_prd_instnc_s has failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 3
\else
\remark '\n **** Truncate table pcm_prd_instnc_s is executed Successfully **** \n'
\endif

/*==============================================================================================================================
=========================================================BEGIN OF TRANSACTION===================================================
================================================================================================================================*/
BEGIN;

/*==============================================================================================================================
==============================inserting the data into PCM_PRD_INSTNC_S  from PQW  tables========================================
================================================================================================================================*/

INSERT INTO hdl_pqw_etl_stage.pcm_prd_instnc_s 
(
prd_instnc_expld_id
,om_ordr_hdr_id
, ordr_nbr_key
, pi_key
, compnt_item_nbr
, compnt_gehc_med_devc_flg
, compnt_non_gehc_med_devc_flg
, hier_lvl
, hier_path
, ordr_cretn_dt
, load_dtm
, update_dtm
, posting_agent
, source_creation_dtm
, source_update_dtm
)
SELECT
COALESCE(d.ORDR_NBR_KEY::varchar,'')||'~'||COALESCE(d.PI_KEY::varchar,'')||'~'||COALESCE(d.COMPNT_ITEM_NBR::varchar,'')||'~'||COALESCE(d.HIER_LVL::varchar,'')||'~'||COALESCE(d.HIER_PATH,'')||'~'||COALESCE(TO_CHAR(d.ORDR_CRETN_DT,'YYYY-MM-DD'),'') AS prd_instnc_expld_id
,d.om_ordr_hdr_id
, d.ordr_nbr_key
, d.pi_key
, d.compnt_item_nbr
, prd.itm_gehc_med_devc_flg
, prd.itm_non_gehc_med_devc_flg
, d.hier_lvl
, d.hier_path
, d.ordr_cretn_dt
, CURRENT_TIMESTAMP(0)
, CURRENT_TIMESTAMP(0)
, 'RSQL_PCM_BOM_DI_EDST_PCM_PRD_INSTNC_S.sh'
, d.load_dtm
, d.update_dtm
    FROM hdl_pqw_fnd_data.prd_instnc_expld_d AS d
    INNER JOIN hdl_pqw_fnd_data.pqw_product_mstr_d AS prd
        ON (prd.itm_nbr_key = d.compnt_item_nbr)
    WHERE d.update_dtm >= (SELECT
        start_date
        FROM hdl_pqw_etl_stage.pqw_ordr_reproc_date_s
        WHERE table_nam = 'PCMBOM~PRD_INSTNC_EXPLD_D_X') 
		AND d.update_dtm <= (SELECT
        end_date
        FROM hdl_pqw_etl_stage.pqw_ordr_reproc_date_s
        WHERE table_nam = 'PCMBOM~PRD_INSTNC_EXPLD_D_X');

\if :ERROR <> 0
\echo 'Insert table pcm_prd_instnc_s got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 4
\else
\remark '\n **** Insert table pcm_prd_instnc_s is executed Successfully **** \n'
\endif

/*==============================================================================================================================
==============================Checking if GEHC MD or NON GEHC medical device is present in a PI=================================
================================================================================================================================*/

UPDATE hdl_pqw_etl_stage.pcm_prd_instnc_s
SET upd_flg =
CASE
    WHEN d1.max_md_flg = 'Yes' OR d1.max_non_md_flg = 'Yes' THEN 'Yes'
    ELSE NULL
END
FROM (SELECT
    pi_key, MAX(compnt_gehc_med_devc_flg) AS max_md_flg, MAX(compnt_non_gehc_med_devc_flg) AS max_non_md_flg
    FROM hdl_pqw_etl_stage.pcm_prd_instnc_s
    GROUP BY 1) AS d1
    WHERE hdl_pqw_etl_stage.pcm_prd_instnc_s.pi_key = d1.pi_key;

\if :ERROR <> 0
\echo 'Update table pcm_prd_instnc_s got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 5
\else
\remark '\n **** Update table pcm_prd_instnc_s is executed Successfully **** \n'
\endif

/*==============================================================================================================================
================================================== creating VOLATILE table =====================================================
================================================================================================================================*/
Drop Table if exists pcm_bom_template_sys_vt;
CREATE TEMPORARY TABLE pcm_bom_template_sys_vt
(pcm_nam CHARACTER VARYING(300) ENCODE RAW collate case_insensitive,
    system_item CHARACTER VARYING(150) NOT NULL ENCODE LZO collate case_insensitive,
    pcm_sys_lvl_id INTEGER ENCODE AZ64)
DISTSTYLE KEY
DISTKEY (pcm_nam)
SORTKEY (pcm_nam);

\if :ERROR <> 0
\echo 'Create temporary table pcm_bom_template_sys_vt got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 6
\else
\remark '\n **** Create temporary table pcm_bom_template_sys_vt is executed Successfully **** \n'
\endif

/*==============================================================================================================================
================================================== inserting the data into VT table ============================================
================================================================================================================================*/

INSERT INTO pcm_bom_template_sys_vt (pcm_nam, system_item, pcm_sys_lvl_id)
SELECT
    pcm_nam, system_item, pcm_sys_lvl_id
    FROM hdl_pqw_etl_stage.pcm_bom_template_s
    GROUP BY 1, 2, 3;

\if :ERROR <> 0
\echo 'Insert table pcm_bom_template_sys_vt got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 7
\else
\remark '\n **** Insert table pcm_bom_template_sys_vt is executed Successfully **** \n'
\endif

/*==============================================================================================================================
==============================Checking if GEHC MD or NON GEHC medical device is present in a PI=================================
================================================================================================================================*/

UPDATE hdl_pqw_etl_stage.pcm_prd_instnc_s 
SET UPD_FLG='Err0'
FROM (SELECT PI_KEY FROM hdl_pqw_etl_stage.pcm_prd_instnc_s
INNER JOIN PCM_BOM_TEMPLATE_SYS_VT
ON SYSTEM_ITEM=COMPNT_ITEM_NBR
WHERE UPD_FLG IS NULL
GROUP BY 1)E
WHERE hdl_pqw_etl_stage.pcm_prd_instnc_s.PI_KEY=E.PI_KEY;

\if :ERROR <> 0
\echo 'Update table pcm_prd_instnc_s got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 8
\else
\remark '\n **** Update table pcm_prd_instnc_s is executed Successfully **** \n'
\endif

/*==============================================================================================================================
=======================================delete the upd_flg as null values from PCM_PRD_INSTNC_S =================================
================================================================================================================================*/

DELETE FROM hdl_pqw_etl_stage.pcm_prd_instnc_s
WHERE upd_flg IS NULL;

\if :ERROR <> 0
\echo 'Delete table pcm_prd_instnc_s got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 9
\else
\remark '\n **** Delete table pcm_prd_instnc_s is executed Successfully **** \n'
\endif

/*==============================================================================================================================
====================================================== Case 2: MD But no Match =================================================
================================================================================================================================*/

DELETE FROM hdl_pqw_etl_stage.pcm_prd_instnc_s
WHERE pi_key NOT IN (SELECT
        d.pi_key
        FROM hdl_pqw_etl_stage.pcm_prd_instnc_s AS d
        INNER JOIN pcm_bom_template_sys_vt AS s
            ON d.compnt_item_nbr = s.system_item
			AND d.upd_flg IS NOT NULL
        GROUP BY 1);

\if :ERROR <> 0
\echo 'Delete table pcm_prd_instnc_s got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 10
\else
\remark '\n **** Delete table pcm_prd_instnc_s is executed Successfully **** \n'
\endif

/*==============================================================================================================================
======================================================= Update SYS_LVL_MED_FLG =================================================
================================================================================================================================*/
UPDATE hdl_pqw_etl_stage.pcm_prd_instnc_s
SET SYS_LVL_MED_FLG=TRIM(S.SYS_LVL_MED_FLG),
PCM_NAM=S.PCM_NAM,
PCM_NAM_CONCAT=S.PCM_NAM,
PCM_SYS_LVL_ID=S.PCM_SYS_LVL_ID
FROM
(
SELECT
PRD_INSTNC_EXPLD_ID,
PI_KEY,
COMPNT_ITEM_NBR,
PCM_NAM,
PCM_SYS_LVL_ID,
SYSTEM_ITEM,
SUM(
CASE
    WHEN ROWNO = 1 THEN 1
    ELSE 0
END)
   OVER (PARTITION BY PI_KEY  ORDER BY COMPNT_ITEM_NBR, ROWNO ROWS UNBOUNDED PRECEDING) AS SYS_LVL_MED_FLG
FROM
(
SELECT 
P.PRD_INSTNC_EXPLD_ID,
P.PI_KEY,
P.COMPNT_ITEM_NBR,
S.PCM_NAM,
S.PCM_SYS_LVL_ID,
S.SYSTEM_ITEM,
ROW_NUMBER() OVER (PARTITION BY P.PI_KEY , P.COMPNT_ITEM_NBR ORDER BY P.COMPNT_ITEM_NBR ) AS ROWNO
FROM hdl_pqw_etl_stage.pcm_prd_instnc_s P
INNER JOIN PCM_BOM_TEMPLATE_SYS_VT S
    ON P.COMPNT_ITEM_NBR=S.SYSTEM_ITEM) INNERQ
QUALIFY ROW_NUMBER() OVER (PARTITION BY PRD_INSTNC_EXPLD_ID ORDER BY 1 ) = 1  ) S
WHERE hdl_pqw_etl_stage.pcm_prd_instnc_s.PRD_INSTNC_EXPLD_ID=S.PRD_INSTNC_EXPLD_ID;

\if :ERROR <> 0
\echo 'Update table pcm_prd_instnc_s got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 11
\else
\remark '\n **** Update table pcm_prd_instnc_s is executed Successfully **** \n'
\endif

/*==============================================================================================================================
================================================== creating VOLATILE table =====================================================
================================================================================================================================*/
Drop Table if exists pcm_prd_instnc_s_vt;
CREATE TEMPORARY TABLE pcm_prd_instnc_s_vt
(   
prd_instnc_expld_id CHARACTER VARYING(1500) ENCODE collate case_insensitive,
om_ordr_hdr_id CHARACTER VARYING(150) ENCODE collate case_insensitive,
    ordr_nbr_key CHARACTER VARYING(150) ENCODE LZO collate case_insensitive,
    pi_key CHARACTER VARYING(750) ENCODE LZO collate case_insensitive,
    compnt_item_nbr CHARACTER VARYING(150) ENCODE LZO collate case_insensitive,
    compnt_gehc_med_devc_flg CHARACTER VARYING(150) ENCODE LZO collate case_insensitive,
    compnt_non_gehc_med_devc_flg CHARACTER VARYING(150) ENCODE LZO collate case_insensitive,
    upd_flg CHARACTER VARYING(150) ENCODE LZO collate case_insensitive,
    sys_lvl_med_flg CHARACTER VARYING(30) ENCODE LZO collate case_insensitive,
    boundary_flg CHARACTER VARYING(30) ENCODE LZO collate case_insensitive,
    ordr_cretn_dt DATE NOT NULL ENCODE AZ64,
    load_dtm TIMESTAMP ENCODE AZ64,
    update_dtm TIMESTAMP ENCODE AZ64,
    posting_agent CHARACTER VARYING(150) ENCODE LZO collate case_insensitive,
    source_creation_dtm TIMESTAMP ENCODE AZ64,
    source_update_dtm TIMESTAMP ENCODE AZ64,
    pcm_nam CHARACTER VARYING(300) ENCODE LZO collate case_insensitive,
    pcm_sys_lvl_id INTEGER ENCODE AZ64,
    pcm_sys_lvl_id_row_num CHARACTER VARYING(300) ENCODE LZO collate case_insensitive)
DISTSTYLE KEY
DISTKEY (PRD_INSTNC_EXPLD_ID)
SORTKEY (PRD_INSTNC_EXPLD_ID);

\if :ERROR <> 0
\echo 'Create temporary table pcm_prd_instnc_s_vt got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 12
\else
\remark '\n **** Create temporary table pcm_prd_instnc_s_vt is executed Successfully **** \n'
\endif

/*==============================================================================================================================
===================================== holds replicated rows of system having multiple PCMs =====================================
================================================================================================================================*/

INSERT INTO pcm_prd_instnc_s_vt (
prd_instnc_expld_id
,om_ordr_hdr_id
, ordr_nbr_key
, pi_key
, compnt_item_nbr
, compnt_gehc_med_devc_flg
, compnt_non_gehc_med_devc_flg
, upd_flg
, sys_lvl_med_flg
, boundary_flg
, ordr_cretn_dt
, load_dtm
, update_dtm
, posting_agent
, source_creation_dtm
, source_update_dtm
, pcm_nam
, pcm_sys_lvl_id
, pcm_sys_lvl_id_row_num
)
SELECT DISTINCT
p.prd_instnc_expld_id
, p.om_ordr_hdr_id
, p.ordr_nbr_key
, p.pi_key
, p.compnt_item_nbr
, p.compnt_gehc_med_devc_flg
, p.compnt_non_gehc_med_devc_flg
, p.upd_flg
, p.sys_lvl_med_flg
, p.boundary_flg
, p.ordr_cretn_dt
, p.load_dtm
, p.update_dtm
, p.posting_agent
, p.source_creation_dtm
, p.source_update_dtm
, s.pcm_nam
, s.pcm_sys_lvl_id
, row_number() OVER (PARTITION BY p.prd_instnc_expld_id ORDER BY 1 NULLS FIRST) AS pcm_sys_lvl_id_row_num
    FROM hdl_pqw_etl_stage.pcm_prd_instnc_s AS p
    INNER JOIN pcm_bom_template_sys_vt AS s
        ON p.compnt_item_nbr = s.system_item;

\if :ERROR <> 0
\echo 'Insert table pcm_prd_instnc_s_vt got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 13
\else
\remark '\n **** Insert table pcm_prd_instnc_s_vt is executed Successfully **** \n'
\endif

/*==============================================================================================================================
============================================== creating VOLATILE table for hold original records ===============================
================================================================================================================================*/
Drop Table if exists pcm_prd_instnc_s_vt1;
CREATE TEMPORARY TABLE pcm_prd_instnc_s_vt1
(   prd_instnc_expld_id CHARACTER VARYING(1500) ENCODE collate case_insensitive,
    om_ordr_hdr_id CHARACTER VARYING(150) ENCODE lzo collate case_insensitive,
    ordr_nbr_key CHARACTER VARYING(150) NOT NULL ENCODE LZO collate case_insensitive,
    pi_key CHARACTER VARYING(750) NOT NULL ENCODE LZO collate case_insensitive,
    compnt_item_nbr CHARACTER VARYING(150) NOT NULL ENCODE LZO collate case_insensitive,
    compnt_gehc_med_devc_flg CHARACTER VARYING(150) ENCODE LZO collate case_insensitive,
    compnt_non_gehc_med_devc_flg CHARACTER VARYING(150) ENCODE LZO collate case_insensitive,
    upd_flg CHARACTER VARYING(150) ENCODE LZO collate case_insensitive,
    sys_lvl_med_flg CHARACTER VARYING(30) ENCODE LZO collate case_insensitive,
    boundary_flg CHARACTER VARYING(30) ENCODE LZO collate case_insensitive,
    ordr_cretn_dt DATE NOT NULL ENCODE AZ64,
    load_dtm TIMESTAMP ENCODE AZ64,
    update_dtm TIMESTAMP ENCODE AZ64,
    posting_agent CHARACTER VARYING(150) ENCODE LZO collate case_insensitive,
    source_creation_dtm TIMESTAMP ENCODE AZ64,
    source_update_dtm TIMESTAMP ENCODE AZ64,
    pcm_nam CHARACTER VARYING(300) ENCODE LZO collate case_insensitive,
    pcm_sys_lvl_id INTEGER ENCODE AZ64,
    pcm_nam_concat CHARACTER VARYING(750) ENCODE LZO collate case_insensitive)
DISTSTYLE KEY
DISTKEY (prd_instnc_expld_id)
SORTKEY (prd_instnc_expld_id);

\if :ERROR <> 0
\echo 'Create temporary table pcm_prd_instnc_s_vt1 got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 14
\else
\remark '\n **** Create temporary table pcm_prd_instnc_s_vt1 is executed Successfully **** \n'
\endif

/*==============================================================================================================================
========================================================== To hold original records ============================================
================================================================================================================================*/

INSERT INTO pcm_prd_instnc_s_vt1 
( prd_instnc_expld_id
,om_ordr_hdr_id
, ordr_nbr_key
, pi_key
, compnt_item_nbr
, compnt_gehc_med_devc_flg
, compnt_non_gehc_med_devc_flg
, upd_flg
, sys_lvl_med_flg
, boundary_flg
, ordr_cretn_dt
, load_dtm
, update_dtm
, posting_agent
, source_creation_dtm
, source_update_dtm
, pcm_nam
, pcm_sys_lvl_id
, pcm_nam_concat
)
SELECT
prd_instnc_expld_id
, om_ordr_hdr_id
, ordr_nbr_key
, pi_key
, compnt_item_nbr
, compnt_gehc_med_devc_flg
, compnt_non_gehc_med_devc_flg
, upd_flg
, sys_lvl_med_flg
, boundary_flg
, ordr_cretn_dt
, load_dtm
, update_dtm
, posting_agent
, source_creation_dtm
, source_update_dtm
, pcm_nam
, pcm_sys_lvl_id
, pcm_nam_concat
    FROM hdl_pqw_etl_stage.pcm_prd_instnc_s
    WHERE prd_instnc_expld_id IN (SELECT DISTINCT
        prd_instnc_expld_id
        FROM pcm_prd_instnc_s_vt);

\if :ERROR <> 0
\echo 'Insert table pcm_prd_instnc_s_vt1 got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 15
\else
\remark '\n **** Insert table pcm_prd_instnc_s_vt1 is executed Successfully **** \n'
\endif

/*==============================================================================================================================
============================================== creating VOLATILE table for hold original records ===============================
================================================================================================================================*/
Drop Table if exists pcm_prd_instnc_vt;
CREATE TEMPORARY TABLE pcm_prd_instnc_vt
(pcm_sys_lvl_id INTEGER ENCODE RAW,
    pcm_nam CHARACTER VARYING(300) ENCODE LZO collate case_insensitive,
    pi_key CHARACTER VARYING(750) ENCODE LZO collate case_insensitive,
    compnt_item_nbr CHARACTER VARYING(150) ENCODE LZO collate case_insensitive,
	prd_instnc_expld_id CHARACTER VARYING(1500) ENCODE LZO collate case_insensitive,
    sys_lvl_med_flg CHARACTER VARYING(30) ENCODE LZO collate case_insensitive,
    md_compnt_item_nbr CHARACTER VARYING(150) ENCODE LZO collate case_insensitive)
DISTSTYLE KEY
DISTKEY (pcm_sys_lvl_id)
SORTKEY (pcm_sys_lvl_id);

\if :ERROR <> 0
\echo 'Create temporary table pcm_prd_instnc_vt got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 16
\else
\remark '\n **** Create temporary table pcm_prd_instnc_vt is executed Successfully **** \n'
\endif

/*==============================================================================================================================
===================== inserting the data into PCM_PRD_INSTNC_VT from PCM_PRD_INSTNC_S ==========================================
================================================================================================================================*/

INSERT INTO pcm_prd_instnc_vt 
(
	pcm_sys_lvl_id
, pcm_nam
, pi_key
, compnt_item_nbr
, prd_instnc_expld_id
, sys_lvl_med_flg
, md_compnt_item_nbr
)
SELECT
    p2.pcm_sys_lvl_id
, p2.pcm_nam
, p.pi_key
, p.compnt_item_nbr
, p.prd_instnc_expld_id
, p2.sys_lvl_med_flg
, p2.compnt_item_nbr AS md_compnt_item_nbr
    FROM hdl_pqw_etl_stage.pcm_prd_instnc_s AS p
    INNER JOIN (SELECT
        pcm_sys_lvl_id, pcm_nam, pi_key, sys_lvl_med_flg, compnt_item_nbr
        FROM pcm_prd_instnc_s_vt
        GROUP BY 1, 2, 3, 4, 5) AS p2
        ON (p2.pi_key = p.pi_key);

\if :ERROR <> 0
\echo 'Insert table pcm_prd_instnc_vt got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 17
\else
\remark '\n **** Insert table pcm_prd_instnc_vt is executed Successfully **** \n'
\endif

/*=================================================================================================================================
=== Assign a seqence number to identify systemetically cases where more than one PCM need to be reffered to do boundary match =====
====================================================================================================================================*/
Drop Table if exists pcm_prd_instnc_vt1;
CREATE TEMPORARY TABLE pcm_prd_instnc_vt1
(pcm_sys_lvl_id INTEGER ENCODE RAW,
    pcm_nam CHARACTER VARYING(300) ENCODE LZO collate case_insensitive,
    pi_key CHARACTER VARYING(750) ENCODE LZO collate case_insensitive,
    compnt_item_nbr CHARACTER VARYING(150) ENCODE LZO collate case_insensitive,
	prd_instnc_expld_id CHARACTER VARYING(1500) ENCODE LZO collate case_insensitive,
    sys_lvl_med_flg CHARACTER VARYING(30) ENCODE LZO collate case_insensitive,
    md_compnt_item_nbr CHARACTER VARYING(150) ENCODE LZO collate case_insensitive,
    seq_num INTEGER ENCODE AZ64)
DISTSTYLE KEY
DISTKEY (pcm_sys_lvl_id)
SORTKEY (pcm_sys_lvl_id);

\if :ERROR <> 0
\echo 'Create temporary table pcm_prd_instnc_vt1 got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 18
\else
\remark '\n **** Create temporary table pcm_prd_instnc_vt1 is executed Successfully **** \n'
\endif

/*=================================================================================================================================
==== Assign a seqence number to identify systemetically cases where more than one PCM need to be reffered to do boundary match ====
====================================================================================================================================*/

INSERT INTO pcm_prd_instnc_vt1 
(
pcm_sys_lvl_id
, pcm_nam
, pi_key
, compnt_item_nbr
, prd_instnc_expld_id
, sys_lvl_med_flg
, md_compnt_item_nbr
, seq_num
)
SELECT
    vt.pcm_sys_lvl_id
, vt.pcm_nam
, vt.pi_key
, vt.compnt_item_nbr
, vt.prd_instnc_expld_id
, vt.sys_lvl_med_flg
, vt.md_compnt_item_nbr
, row_number() OVER (PARTITION BY prd_instnc_expld_id ORDER BY sys_lvl_med_flg NULLS FIRST) AS seq_num
    FROM pcm_prd_instnc_vt AS vt
    INNER JOIN hdl_pqw_etl_stage.pcm_bom_template_s AS ts
        ON (ts.pcm_sys_lvl_id = vt.pcm_sys_lvl_id 
		AND vt.compnt_item_nbr = ts.system_boundary_item);

\if :ERROR <> 0
\echo 'Insert table pcm_prd_instnc_vt1 got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 19
\else
\remark '\n **** Insert table pcm_prd_instnc_vt1 is executed Successfully **** \n'
\endif

/*==============================================================================================================================
============================================== creating the VOLATILE table  ====================================================
================================================================================================================================*/
Drop table if exists pcm_prd_instnc_vt3;
CREATE TEMPORARY TABLE pcm_prd_instnc_vt3
(   prd_instnc_expld_id CHARACTER VARYING(1500) ENCODE LZO collate case_insensitive,
    pi_key CHARACTER VARYING(750) ENCODE LZO collate case_insensitive,
    boundary_flg CHARACTER VARYING(50) ENCODE LZO collate case_insensitive,
    seq_num CHARACTER VARYING(50) ENCODE LZO collate case_insensitive,
    loop_counter INTEGER ENCODE AZ64,
    pcm_sys_lvl_id INTEGER ENCODE AZ64,
    pcm_nam CHARACTER VARYING(300) ENCODE LZO collate case_insensitive,
    pcm_nam_concat CHARACTER VARYING(750) ENCODE LZO collate case_insensitive)
DISTSTYLE KEY
DISTKEY (prd_instnc_expld_id)
SORTKEY (prd_instnc_expld_id);

\if :ERROR <> 0
\echo 'Create temporary table pcm_prd_instnc_vt3 got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 20
\else
\remark '\n **** Create temporary table pcm_prd_instnc_vt3 is executed Successfully **** \n'
\endif


/*==============================================================================================================================
===================================== inserting the data into PCM_PRD_INSTNC_VT3 from PCM_PRD_INSTNC_VT1  ======================
================================================================================================================================*/

INSERT INTO pcm_prd_instnc_vt3 
( prd_instnc_expld_id
, boundary_flg
, seq_num
, loop_counter
, pcm_sys_lvl_id
, pcm_nam
, pcm_nam_concat
)
WITH RECURSIVE pcm_recursive ( prd_instnc_expld_id,sys_lvl_med_flg, seq_num, loop_counter, prev_sys_lvl_med_flg, pcm_sys_lvl_id, pcm_nam, prev_pcm_nam, pcm_nam_concat)
AS (SELECT
     prd_instnc_expld_id
     ,sys_lvl_med_flg
, seq_num
, 1 AS loop_counter
, sys_lvl_med_flg AS prev_sys_lvl_med_flg
, pcm_sys_lvl_id
, pcm_nam
, pcm_nam AS prev_pcm_nam
, pcm_nam AS pcm_nam_concat
    FROM pcm_prd_instnc_vt1
    WHERE seq_num = 1
UNION ALL
SELECT
pcm_recursive.prd_instnc_expld_id,
      CASE
        WHEN pcm_recursive.prev_sys_lvl_med_flg = vt.sys_lvl_med_flg THEN pcm_recursive.sys_lvl_med_flg
        ELSE pcm_recursive.sys_lvl_med_flg || ',' || vt.sys_lvl_med_flg
    END AS sys_lvl_med_flg, 
	vt.seq_num, 
	pcm_recursive.loop_counter + 1, 
	vt.sys_lvl_med_flg AS prev_sys_lvl_med_flg, 
	vt.pcm_sys_lvl_id, 
	vt.pcm_nam, 
	vt.pcm_nam AS prev_pcm_nam,
    CASE
        WHEN pcm_recursive.prev_pcm_nam = vt.pcm_nam THEN pcm_recursive.pcm_nam_concat
        ELSE pcm_recursive.pcm_nam_concat || ',' || vt.pcm_nam
    END AS pcm_nam_concat
    FROM pcm_prd_instnc_vt1 AS vt
    INNER JOIN pcm_recursive
        ON (pcm_recursive.prd_instnc_expld_id = vt.prd_instnc_expld_id)
    WHERE vt.seq_num > 1 AND
        pcm_recursive.loop_counter + 1 = vt.seq_num)
SELECT
qualify_subquery.prd_instnc_expld_id
, qualify_subquery.sys_lvl_med_flg
, qualify_subquery.seq_num
, qualify_subquery.loop_counter
, qualify_subquery.pcm_sys_lvl_id
, qualify_subquery.pcm_nam
, qualify_subquery.pcm_nam_concat
    FROM (SELECT prd_instnc_expld_id,
        sys_lvl_med_flg, 
		CAST (CAST (seq_num AS CHARACTER VARYING(10)) AS CHARACTER VARYING(10)) AS seq_num, 
		CAST (loop_counter AS INTEGER) AS loop_counter, 
		pcm_sys_lvl_id, 
		pcm_nam, 
		pcm_nam_concat, 
		row_number() OVER (PARTITION BY prd_instnc_expld_id ORDER BY loop_counter DESC NULLS LAST) AS qualify_expression_1
        FROM pcm_recursive) AS qualify_subquery
    WHERE qualify_expression_1 = 1;

\if :ERROR <> 0
\echo 'Insert table pcm_prd_instnc_vt3 got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 21
\else
\remark '\n **** Insert table pcm_prd_instnc_vt3 is executed Successfully **** \n'
\endif

/*==============================================================================================================================
===================================== updating the values in PCM_PRD_INSTNC_S ==================================================
================================================================================================================================*/

UPDATE hdl_pqw_etl_stage.pcm_prd_instnc_s
SET boundary_flg = vt3.boundary_flg
, pcm_nam = vt3.pcm_nam
, pcm_sys_lvl_id = vt3.pcm_sys_lvl_id
, pcm_nam_concat = vt3.pcm_nam_concat
FROM pcm_prd_instnc_vt3 AS vt3
    WHERE vt3.prd_instnc_expld_id = hdl_pqw_etl_stage.pcm_prd_instnc_s.prd_instnc_expld_id;

\if :ERROR <> 0
\echo 'Update table pcm_prd_instnc_s got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 22
\else
\remark '\n **** Update table pcm_prd_instnc_s is executed Successfully **** \n'
\endif

/*==============================================================================================================================
===================================== updating the values in PCM_PRD_INSTNC_S ==================================================
================================================================================================================================*/
UPDATE hdl_pqw_etl_stage.pcm_prd_instnc_s 
SET PCM_NAM = DER.PCM_NAM
FROM (
SELECT PCM.PI_KEY,
PCM.SYS_LVL_MED_FLG,
PCM.PCM_NAM,
COUNT(PCM.MD_COMPNT_ITEM_NBR) BOUNDARY_MATCH_COUNT
FROM    PCM_PRD_INSTNC_VT1  PCM
INNER JOIN PCM_PRD_INSTNC_S_VT1  PCM_DUP ON ( PCM_DUP.PI_KEY = PCM.PI_KEY)
GROUP BY PCM.PI_KEY, PCM.SYS_LVL_MED_FLG, PCM.PCM_NAM
QUALIFY ROW_NUMBER() OVER (PARTITION BY PCM.PI_KEY, PCM.SYS_LVL_MED_FLG
ORDER BY COUNT(PCM.MD_COMPNT_ITEM_NBR)  DESC) = 1) DER
WHERE hdl_pqw_etl_stage.pcm_prd_instnc_s.PI_KEY = DER.PI_KEY
    AND hdl_pqw_etl_stage.pcm_prd_instnc_s.SYS_LVL_MED_FLG = DER.SYS_LVL_MED_FLG;


\if :ERROR <> 0
\echo 'Update table pcm_prd_instnc_s got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 23
\else
\remark '\n **** Update table pcm_prd_instnc_s is executed Successfully **** \n'
\endif

/*==============================================================================================================================
===================================== updating the values in PCM_PRD_INSTNC_S ==================================================
================================================================================================================================*/

UPDATE hdl_pqw_etl_stage.pcm_prd_instnc_s
SET pcm_nam = NULL
    WHERE boundary_flg IS NOT NULL;

\if :ERROR <> 0
\echo 'Update table pcm_prd_instnc_s got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 24
\else
\remark '\n **** Update table pcm_prd_instnc_s is executed Successfully **** \n'
\endif

/*==============================================================================================================================
============================================== creating the VOLATILE table  ====================================================
================================================================================================================================*/
Drop table if exists pcm_prd_instnc_system_pcm_vt;
CREATE TEMPORARY TABLE pcm_prd_instnc_system_pcm_vt
AS
(WITH RECURSIVE sys_pcm_nam_concat (prd_instnc_expld_id,sys_lvl_med_flg, pcm_nam, loop_counter, prev_pcm_nam, pcm_nam_concat)
AS (SELECT
prd_instnc_expld_id
, sys_lvl_med_flg
, pcm_nam
, pcm_sys_lvl_id_row_num AS loop_counter
, pcm_nam AS prev_pcm_nam
, pcm_nam AS pcm_nam_concat
    FROM pcm_prd_instnc_s_vt AS pcm
    WHERE pcm_sys_lvl_id_row_num = 1
UNION ALL
SELECT
     vt.prd_instnc_expld_id,vt.sys_lvl_med_flg, vt.pcm_nam, sys.loop_counter + 1 AS loop_counter, vt.pcm_nam AS prev_pcm_nam,
    CASE
        WHEN sys.prev_pcm_nam = vt.pcm_nam THEN sys.pcm_nam_concat
        ELSE sys.pcm_nam_concat || ',' || vt.pcm_nam
    END AS pcm_nam_concat
    FROM sys_pcm_nam_concat AS sys
    INNER JOIN pcm_prd_instnc_s_vt AS vt
        ON (sys.prd_instnc_expld_id = vt.prd_instnc_expld_id)
    WHERE vt.pcm_sys_lvl_id_row_num > 1 AND    
    sys.loop_counter + 1 = vt.pcm_sys_lvl_id_row_num)
SELECT  
       prd_instnc_expld_id,sys_lvl_med_flg, pcm_nam, loop_counter, prev_pcm_nam, pcm_nam_concat
    FROM (SELECT
        qualify_star.*, row_number() OVER (PARTITION BY prd_instnc_expld_id ORDER BY loop_counter DESC NULLS LAST) AS qualify_expression_1
        FROM sys_pcm_nam_concat AS qualify_star) AS qualify_subquery
    WHERE qualify_expression_1 = 1);

\if :ERROR <> 0
\echo 'Create temporary table pcm_prd_instnc_system_pcm_vt got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 25
\else
\remark '\n **** Create temporary table pcm_prd_instnc_system_pcm_vt is executed Successfully **** \n'
\endif

/*==============================================================================================================================
===================================== updating the values in PCM_PRD_INSTNC_S ==================================================
================================================================================================================================*/

UPDATE hdl_pqw_etl_stage.pcm_prd_instnc_s
SET pcm_nam_concat = sys_pcm.pcm_nam_concat
FROM pcm_prd_instnc_system_pcm_vt AS sys_pcm
    WHERE sys_pcm.prd_instnc_expld_id = hdl_pqw_etl_stage.pcm_prd_instnc_s.prd_instnc_expld_id;

\if :ERROR <> 0
\echo 'Update table pcm_prd_instnc_s got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 26
\else
\remark '\n **** Update table pcm_prd_instnc_s is executed Successfully **** \n'
\endif

/*==============================================================================================================================
===================================== updating the values in PCM_PRD_INSTNC_S ==================================================
================================================================================================================================*/

UPDATE hdl_pqw_etl_stage.pcm_prd_instnc_s
SET upd_flg = 'Err1'
FROM (SELECT
    pi_key
    FROM (SELECT
        pi_key, MAX(boundary_flg) AS bdry_flg
        FROM hdl_pqw_etl_stage.pcm_prd_instnc_s
        GROUP BY 1) AS x
    WHERE bdry_flg IS NULL) AS y
    WHERE hdl_pqw_etl_stage.pcm_prd_instnc_s.pi_key = y.pi_key;

\if :ERROR <> 0
\echo 'Update table pcm_prd_instnc_s got failed '
\echo 'Error Code -'
\echo :ERRORCODE
\remark :LAST_ERROR_MESSAGE
\exit 27
\else
\remark '\n **** Update table pcm_prd_instnc_s is executed Successfully **** \n'
\endif

/*==========================================================================================================================
==================================================END OF TRANSACTION========================================================
============================================================================================================================*/
commit;


\echo '\n-----MAIN EXECUTION LOG FINISHED HERE-----\n'
\exit 0

EOF

rsqlexitcode=$?
echo Exited with error code $rsqlexitcode

echo End Time : $(date +"%T.%N")
end=`date +%s`
exec=$(($end - $start))
echo Total Time Taken : $exec seconds

python3  /ops/common/scripts/send_sfn_token.py $token $script_name $rsqlexitcode $log_file_name > /ops/common/logs/py.log

exit $rsqlexitcode
