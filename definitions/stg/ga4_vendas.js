publish(`ga4_vendas`, {
    type: "incremental",
    schema: "monitoramento",
     tags: ["ga4", "vendas", "web","stg"] 
}).query(ctx => {
    const tenantQueries = constants.GA4_VENDAS
        .map(datasetGA4 => {
            let day_r = 1;


            const ga4_query = `
SELECT
  PARSE_DATE('%Y%m%d', event_date) AS data,
  platform plataforma,
  "vendas" metrica,
  items[SAFE_OFFSET(0)].item_name AS produto,
  count(*) valor_metrica,
  current_datetime data_ingestao
  FROM ${ctx.ref(datasetGA4, "events_*")}
WHERE
  event_name = "purchase"
  AND platform = "WEB"
  AND _TABLE_SUFFIX = FORMAT_DATETIME('%Y%m%d', DATE(DATE_SUB(current_date('America/Sao_Paulo'), INTERVAL ${day_r} DAY)))
GROUP BY ALL


UNION ALL

SELECT
  PARSE_DATE('%Y%m%d', event_date) AS data,
  platform plataforma,
  "vendas_origem" metrica,
  dp.origem  AS produto,
  count(*) valor_metrica,
  current_datetime data_ingestao
  FROM ${ctx.ref(datasetGA4, "events_*")}
  LEFT JOIN ${ctx.ref("monitoramento", "vendas_de_para")}  dp on cast(dp.origem_id as int64) = (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "origem_id")
WHERE
  event_name = "purchase"
  AND platform = "WEB"
  AND _TABLE_SUFFIX = FORMAT_DATETIME('%Y%m%d', DATE(DATE_SUB(current_date('America/Sao_Paulo'), INTERVAL ${day_r} DAY)))
GROUP BY ALL



`;

            return ga4_query;
        });

    const finalQuery = `
    ${tenantQueries.join("\nUNION ALL\n")}
  `;

    return finalQuery;
});
