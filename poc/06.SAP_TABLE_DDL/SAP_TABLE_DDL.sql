

desc table dwdm.inf01.ifpf_sap_ctpr_fctr_c;
CREATE OR REPLACE TABLE  sap.SAPSR3.CSKU(
     MANDT	VARCHAR
    ,SPRAS	VARCHAR
    ,KTOPL	VARCHAR
    ,KSTAR	VARCHAR
    ,KTEXT	VARCHAR
    ,LTEXT	VARCHAR
    ,MCTXT	VARCHAR
);

desc table dwdm.inf01.ifpf_sap_ctpr_fctr_l3_c;
CREATE OR REPLACE TABLE  sap.SAPSR3.ZCOT0031 (
       MANDT varchar
     , KSTAR_L3 varchar
     , KTEXT_L3 varchar
)
;

desc table dwdm.inf01.ifpf_sap_ctpr_fctr_l4_c;
CREATE OR REPLACE TABLE  sap.SAPSR3.CSKA (
      MANDT	VARCHAR
    , KTOPL	VARCHAR
    , KSTAR	VARCHAR
    , ERSDA	VARCHAR
    , USNAM	VARCHAR
    , STEKZ	VARCHAR
    , ZAHKZ	VARCHAR
    , KSTSN	VARCHAR
    , FUNC_AREA	VARCHAR
)
;

desc table sap.SAPSR3.ZCOT596M;

desc table dwdm.inf01.ifpf_sap_ctpr_fctr_mstr_c;
CREATE OR REPLACE TABLE  sap.SAPSR3.ZCOT596M (
      IOACC	VARCHAR
    , IOACCTXT	VARCHAR
    , ZCLASSSTXT	VARCHAR
    , ZCLASSMTXT	VARCHAR
    , ZCLASSLTXT	VARCHAR
    , GUBUN	VARCHAR
)
;



desc table dwdm.inf01.ifpf_sap_dv_c;
CREATE OR REPLACE TABLE  sap.SAPSR3.DD07T (
      DOMNAME	VARCHAR
    , DDLANGUAGE	VARCHAR
    , AS4LOCAL	VARCHAR
    , VALPOS	VARCHAR
    , AS4VERS	VARCHAR
    , DDTEXT	VARCHAR
    , DOMVAL_LD	VARCHAR
    , DOMVAL_HD	VARCHAR
    , DOMVALUE_L	VARCHAR
    , VALPOS_NUM	VARCHAR
)
;

desc table dwdm.inf01.ifpf_sap_ordr_c;
CREATE OR REPLACE TABLE  sap.SAPSR3.AUFK (
      AUFNR	VARCHAR
    , KTEXT	VARCHAR
)
;


desc table dwdm.inf01.ifpf_sap_ordr_type_c;
CREATE OR REPLACE TABLE  sap.SAPSR3.v_auart (
      CLIENT	VARCHAR
    , AUART	VARCHAR
    , AUTYP	VARCHAR
    , TXT	VARCHAR
    , SPRAS	VARCHAR
)
;




desc table dwdm.inf01.ifpf_sap_org_type_m;
CREATE OR REPLACE TABLE  sap.SAPSR3.zcot0501n (
          MANDT	VARCHAR
        , KOKRS	VARCHAR
        , KOSTL	VARCHAR
        , DATBI	VARCHAR
        , ZKSDO	VARCHAR
        , DATAB	VARCHAR
        , BUKRS	VARCHAR
        , DATUM	VARCHAR
)
;