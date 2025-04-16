publish(`ga4`, {
    type: "incremental",
    schema: "monitoramento"
}).query(ctx => {
    const tenantQueries = constants.GA4_VENDAS
        .map(datasetGA4 => {
            let day_r = 2;


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
ORDER BY 4 desc
`;

            return ga4_query;
        });

    const finalQuery = `
    ${tenantQueries.join("\nUNION ALL\n")}
  `;

    return finalQuery;
});
