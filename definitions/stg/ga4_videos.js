publish(`ga4_videos`, {
    type: "incremental",
    schema: "monitoramento",
    tags: ["ga4", "videos", "video_views", "video_playtime","stg"]
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
                                  CASE
                                    WHEN REGEXP_CONTAINS(LOWER(consumption_environment), 'amp') THEN 'amp'
                                    WHEN REGEXP_CONTAINS(LOWER(platform_1), 'tv|roku') AND REGEXP_CONTAINS(LOWER(platform_1), 'android|fire') THEN 'tv-android'
                                    WHEN REGEXP_CONTAINS(LOWER(platform_1), 'smartapp|slim') THEN 'tv_smart_e_outras'
                                    WHEN REGEXP_CONTAINS(LOWER(platform_1), 'roku') AND NOT REGEXP_CONTAINS(LOWER(platform_1), 'android|fire') THEN 'tv-roku'
                                    WHEN REGEXP_CONTAINS(LOWER(platform_1), 'tv') AND NOT REGEXP_CONTAINS(LOWER(platform_1), 'android|fire') THEN 'tv_smart_e_outras'
                                    WHEN LOWER(platform_1) LIKE '%app%' AND REGEXP_CONTAINS(LOWER(platform_1), 'android|fire') THEN 'app-mobile-android'
                                    WHEN LOWER(platform_1) LIKE '%app%' AND REGEXP_CONTAINS(LOWER(platform_1), 'ios') THEN 'app-mobile-ios'
                                    WHEN LOWER(platform_1) LIKE '%web%' AND REGEXP_CONTAINS(LOWER(platform_1), 'android|ios') THEN 'web-mobile'
                                    WHEN LOWER(platform_1) LIKE '%web%' AND LOWER(platform_1) LIKE '%desktop%' THEN 'web-desktop'
                                    WHEN REGEXP_CONTAINS(LOWER(consumption_environment), 'web|amp') AND LOWER(device_category) LIKE '%desktop%' THEN 'web-desktop'
                                    WHEN REGEXP_CONTAINS(LOWER(consumption_environment), 'web') AND LOWER(device_category) LIKE '%mobile%' THEN 'web-mobile'
                                    ELSE 'outros'
                                  END AS plataforma,
                                  "video_views" metrica,
                                  '${horizonTenant}' produto,
                                  SUM(video_views) AS valor_metrica,
                                  CURRENT_DATETIME data_ingestao
                                FROM (
                                  SELECT
                                    PARSE_DATE('%Y%m%d', event_date) AS data,
                                    LOWER(device.category) AS device_category,
                                    LOWER(
                                      (SELECT value.string_value
                                      FROM UNNEST(user_properties)
                                      WHERE key = "consumption_environment")
                                    ) AS consumption_environment,

                                    LOWER(
                                      \`gglobo-globoplay-dados-hdg-prd.udfs.plataforma_ga_detalhe_tecnologias\`(
                                        IFNULL(
                                          IF(
                                            LOWER(
                                              (SELECT value.string_value
                                              FROM UNNEST(user_properties)
                                              WHERE key = "consumption_environment")
                                            ) = 'amp', 
                                            'web', 
                                            LOWER(
                                              (SELECT value.string_value
                                              FROM UNNEST(user_properties)
                                              WHERE key = "consumption_environment")
                                            )
                                          ),
                                          device.category
                                        ),
                                        if('${horizonTenant}' ="globoplay",LOWER((SELECT value.string_value FROM UNNEST(user_properties) WHERE key = "platform")),platform),
                                        LOWER(
                                          IFNULL(
                                            (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = "tv_park"),
                                            IFNULL(
                                              (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "tv_park"),
                                              IFNULL(
                                                (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = "tv_stack"),
                                                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "tv_stack")
                                              )
                                            )
                                          )
                                        ),
                                        LOWER(
                                          IFNULL(
                                            (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = "tv_maker"),
                                            IFNULL(
                                              (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "tv_maker"),
                                              device.mobile_brand_name
                                            )
                                          )
                                        ),
                                        LOWER(
                                          IFNULL(
                                            (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = "tv_model"),
                                            IFNULL(
                                              (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "tv_model"),
                                              device.mobile_model_name
                                            )
                                          )
                                        ),
                                        LOWER(
                                          IFNULL(
                                            LOWER((SELECT value.string_value FROM UNNEST(user_properties) WHERE key = "tv_so")),
                                            IFNULL(
                                              LOWER((SELECT value.string_value FROM UNNEST(event_params) WHERE key = "tv_so")),
                                              LOWER(device.operating_system)
                                            )
                                          )
                                        ),
                                        LOWER(
                                          IFNULL(
                                            (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = "app_version"),
                                            IFNULL(
                                              (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "app_version"),
                                              app_info.version
                                            )
                                          )
                                        )
                                      )
                                    ) AS platform_1,

                                    COUNT(*) AS video_views
                                  FROM ${ctx.ref(temp_tenant, "events_*")}
                                  WHERE 
                                    event_name = "video_start"
                                    AND _TABLE_SUFFIX = FORMAT_DATETIME(
                                      '%Y%m%d',
                                      DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL ${day_r} DAY)
                                    )
                                  GROUP BY 1,2,3,4
                                ) dados_ga4
                                GROUP BY ALL

                                UNION ALL 

                                SELECT
                                  data,
                                  CASE
                                    WHEN REGEXP_CONTAINS(LOWER(consumption_environment), 'amp') THEN 'amp'
                                    WHEN REGEXP_CONTAINS(LOWER(platform_1), 'tv|roku') AND REGEXP_CONTAINS(LOWER(platform_1), 'android|fire') THEN 'tv-android'
                                    WHEN REGEXP_CONTAINS(LOWER(platform_1), 'smartapp|slim') THEN 'tv_smart_e_outras'
                                    WHEN REGEXP_CONTAINS(LOWER(platform_1), 'roku') AND NOT REGEXP_CONTAINS(LOWER(platform_1), 'android|fire') THEN 'tv-roku'
                                    WHEN REGEXP_CONTAINS(LOWER(platform_1), 'tv') AND NOT REGEXP_CONTAINS(LOWER(platform_1), 'android|fire') THEN 'tv_smart_e_outras'
                                    WHEN LOWER(platform_1) LIKE '%app%' AND REGEXP_CONTAINS(LOWER(platform_1), 'android|fire') THEN 'app-mobile-android'
                                    WHEN LOWER(platform_1) LIKE '%app%' AND REGEXP_CONTAINS(LOWER(platform_1), 'ios') THEN 'app-mobile-ios'
                                    WHEN LOWER(platform_1) LIKE '%web%' AND REGEXP_CONTAINS(LOWER(platform_1), 'android|ios') THEN 'web-mobile'
                                    WHEN LOWER(platform_1) LIKE '%web%' AND LOWER(platform_1) LIKE '%desktop%' THEN 'web-desktop'
                                    WHEN REGEXP_CONTAINS(LOWER(consumption_environment), 'web|amp') AND LOWER(device_category) LIKE '%desktop%' THEN 'web-desktop'
                                    WHEN REGEXP_CONTAINS(LOWER(consumption_environment), 'web') AND LOWER(device_category) LIKE '%mobile%' THEN 'web-mobile'
                                    ELSE 'outros'
                                  END AS plataforma,
                                  "video_playtime" metrica,
                                  '${horizonTenant}' produto,
                                  CAST(SUM(video_playtime) AS INT64) AS valor_metrica,
                                  CURRENT_DATETIME data_ingestao
                                FROM (
                                  SELECT
                                    PARSE_DATE('%Y%m%d', event_date) AS data,
                                    LOWER(device.category) AS device_category,
                                    LOWER(
                                      (SELECT value.string_value
                                      FROM UNNEST(user_properties)
                                      WHERE key = "consumption_environment")
                                    ) AS consumption_environment,

                                    LOWER(
                                      \`gglobo-globoplay-dados-hdg-prd.udfs.plataforma_ga_detalhe_tecnologias\`(
                                        IFNULL(
                                          IF(
                                            LOWER(
                                              (SELECT value.string_value
                                              FROM UNNEST(user_properties)
                                              WHERE key = "consumption_environment")
                                            ) = 'amp', 
                                            'web', 
                                            LOWER(
                                              (SELECT value.string_value
                                              FROM UNNEST(user_properties)
                                              WHERE key = "consumption_environment")
                                            )
                                          ),
                                          device.category
                                        ),
                                        if('${horizonTenant}' ="globoplay",LOWER((SELECT value.string_value FROM UNNEST(user_properties) WHERE key = "platform")),platform),
                                        LOWER(
                                          IFNULL(
                                            (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = "tv_park"),
                                            IFNULL(
                                              (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "tv_park"),
                                              IFNULL(
                                                (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = "tv_stack"),
                                                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "tv_stack")
                                              )
                                            )
                                          )
                                        ),
                                        LOWER(
                                          IFNULL(
                                            (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = "tv_maker"),
                                            IFNULL(
                                              (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "tv_maker"),
                                              device.mobile_brand_name
                                            )
                                          )
                                        ),
                                        LOWER(
                                          IFNULL(
                                            (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = "tv_model"),
                                            IFNULL(
                                              (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "tv_model"),
                                              device.mobile_model_name
                                            )
                                          )
                                        ),
                                        LOWER(
                                          IFNULL(
                                            LOWER((SELECT value.string_value FROM UNNEST(user_properties) WHERE key = "tv_so")),
                                            IFNULL(
                                              LOWER((SELECT value.string_value FROM UNNEST(event_params) WHERE key = "tv_so")),
                                              LOWER(device.operating_system)
                                            )
                                          )
                                        ),
                                        LOWER(
                                          IFNULL(
                                            (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = "app_version"),
                                            IFNULL(
                                              (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "app_version"),
                                              app_info.version
                                            )
                                          )
                                        )
                                      )
                                    ) AS platform_1,

                                        SUM(
                                  CASE 
                                    WHEN event_name IN ('video_complete', 'video_pause', 'video_milestone') THEN
                                      GREATEST(
                                        0, 
                                        LEAST(
                                          (SELECT COALESCE(ep.value.int_value, ep.value.float_value, ep.value.double_value) 
                                          FROM UNNEST(event_params) AS ep 
                                          WHERE ep.key = "video_playtime"), 
                                          60
                                        )
                                      )
                                    ELSE 0
                                  END
                                ) AS video_playtime

                                  FROM ${ctx.ref(temp_tenant, "events_*")}
                                  WHERE 
                                    event_name in ("video_complete","video_pause","video_milestone")
                                    AND _TABLE_SUFFIX = FORMAT_DATETIME(
                                      '%Y%m%d',
                                      DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL ${day_r} DAY)
                                    )
                                  GROUP BY 1,2,3,4
                                ) dados_ga4
                                GROUP BY ALL
      `;

            return ga4_query;
        });

    const finalQuery = `
    ${tenantQueries.join("\nUNION ALL\n")}
  `;

    return finalQuery;
});
