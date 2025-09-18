publish(`ga4_completude_globo_id`, {
    type: "incremental",
    schema: "monitoramento",
    tags: ["ga4", "completude", "pageviews", "stg", "globo_id"]
}).query(ctx => {
    const tenantQueries = constants.GA4_TENANT
        .filter(horizonTenant => horizonTenant !== "gigagloob")
        .map(horizonTenant => {
            let temp_tenant = "";
            let day_r = 1;
            if (horizonTenant == "globoplay") {
                temp_tenant = "analytics_153317366";
            } else if (horizonTenant == "g1") {
                temp_tenant = "analytics_153398069";
            } else if (horizonTenant == "gshow") {
                temp_tenant = "analytics_153404207";
            } else if (horizonTenant == "receitas") {
                temp_tenant = "analytics_290405147";
            } else if (horizonTenant == "gigagloob") {
                temp_tenant = "analytics_309670617";
            } else if (horizonTenant == "ge") {
                temp_tenant = "analytics_153402505";
            } else if (horizonTenant == "home_globo") {
                temp_tenant = "analytics_290417375";
            } else if (horizonTenant == "vitrine") {
                temp_tenant = "analytics_329311935";
            } else if (horizonTenant == "cartola") {
                temp_tenant = "analytics_153734558";
            }

            const ga4_query = `
                                SELECT
                                data,
                                COALESCE((
                                    SELECT
                                    value.string_value
                                    FROM
                                    UNNEST(event_params)
                                    WHERE
                                    KEY = 'consumption_environment' ), '') AS plataforma,
                                'page_views_com_globo_id' AS metrica,
                                '${horizonTenant}' produto,
                                COUNTIF(user_code_hit IS NOT NULL) AS valor_metrica,
                                current_datetime data_ingestao
                                FROM (
                                SELECT
                                    PARSE_DATE('%Y%m%d', event_date) AS data,
                                    platform,
                                    event_params,
                                    (
                                    SELECT
                                    COALESCE(
                                        CASE
                                        WHEN TRIM(value.string_value) != '' THEN value.string_value
                                        ELSE NULL
                                    END
                                        , CAST(value.int_value AS STRING), CAST(value.float_value AS STRING), CAST(value.double_value AS STRING) )
                                    FROM
                                    UNNEST(event_params)
                                    WHERE
                                    KEY = 'user_code_hit' ) AS user_code_hit
                                    FROM ${ctx.ref(temp_tenant, "events_*")}
                                WHERE
                                    event_name = 'page_view'   AND _TABLE_SUFFIX = FORMAT_DATETIME('%Y%m%d', DATE(DATE_SUB(current_date('America/Sao_Paulo'), INTERVAL ${day_r} DAY))))
                                GROUP BY
                                data,
                                plataforma
                                UNION ALL
                                SELECT
                                data,
                                COALESCE((
                                    SELECT
                                    value.string_value
                                    FROM
                                    UNNEST(event_params)
                                    WHERE
                                    KEY = 'consumption_environment' ), '') AS plataforma,
                                'page_views_sem_globo_id' AS metrica,
                                '${horizonTenant}' produto,
                                COUNTIF(user_code_hit IS NULL) AS valor_metrica,
                                current_datetime data_ingestao
                                FROM (
                                SELECT
                                    PARSE_DATE('%Y%m%d', event_date) AS data,
                                    platform,
                                    event_params,
                                    (
                                    SELECT
                                    COALESCE(
                                        CASE
                                        WHEN TRIM(value.string_value) != '' THEN value.string_value
                                        ELSE NULL
                                    END
                                        , CAST(value.int_value AS STRING), CAST(value.float_value AS STRING), CAST(value.double_value AS STRING) )
                                    FROM
                                    UNNEST(event_params)
                                    WHERE
                                    KEY = 'user_code_hit' ) AS user_code_hit
                                    FROM ${ctx.ref(temp_tenant, "events_*")}
                                WHERE
                                    event_name = 'page_view'   AND _TABLE_SUFFIX = FORMAT_DATETIME('%Y%m%d', DATE(DATE_SUB(current_date('America/Sao_Paulo'), INTERVAL ${day_r} DAY))))
                                GROUP BY
                                data,
                                plataforma
      `;

            return ga4_query;
        });

    const finalQuery = `
    ${tenantQueries.join("\nUNION ALL\n")}
  `;

    return finalQuery;
});
