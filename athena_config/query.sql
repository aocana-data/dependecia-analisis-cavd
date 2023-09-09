-- WITH      merge_data AS (
--           SELECT    DISTINCT
--                     -- cols requeridas
--                     tbp."id_broker"
--                   , tbp."cuit_del_empleador"
--                   , tbp."fecha_inicio"
--                   , CASE
--                               WHEN (tbp."fecha_fin" IS NULL) THEN DATE '9999-12-31'
--                               ELSE tbp."fecha_fin"
--                     END AS fecha_fin
--           FROM      "caba-piba-staging-zone-db"."tbp_typ_def_registro_laboral_formal" tbp
--           )
--         , typ_kpi_users AS (
--           SELECT   
--                     /*joiner*/
--                     tku."id_broker"
--                     /*CAMPOS PARA SEGMENTACION*/
--                   , tku."tipo_doc_broker"
--                   , tku."documento_broker"
--                   , tku."edad"
--                   , tku."genero_broker"
--                   , tku."nacionalidad_broker"
--                   , tku."tipo_programa"
--                   , tku."sector_productivo"
--                   , CASE
--                               WHEN tku."base_origen_vecino" IS NULL THEN 'NO DETERMINADA' --vecino
--                               WHEN TRIM(tku."base_origen_vecino") LIKE '' THEN 'NO DETERMINADA'
--                               ELSE TRIM(tku."base_origen_vecino")
--                     END base_origen
--           FROM      "caba-piba-staging-zone-db"."view_typ_tmp_kpi_1" AS tku
--           )
--         , typ_kpi_users_joinner AS (
--           SELECT    id_broker
--                   , usrs.*
--                   , laboral.*
--           FROM      merge_data AS laboral
--           LEFT JOIN typ_kpi_users AS usrs USING (id_broker)
--           )
-- SELECT    *
-- FROM      typ_kpi_users_joinner
-- LIMIT     1000;  
SELECT    id_broker
        , cursada_estado_beneficiario
        , cursada_fecha_inicio
        , cursada_fecha_egreso
        , base_origen_cursada
        , tipo_programa
FROM      "caba-piba-staging-zone-db"."view_typ_tmp_beneficiarios_educacion"
WHERE     (base_origen_cursada IS NOT NULL)
LIMIT     1000;