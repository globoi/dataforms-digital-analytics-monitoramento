publish(`horizon_pageviews`, {
  type: "incremental",
  schema: "monitoramento",
  tags: ["horizon", "pageviews", "stg"]
}).query(ctx => {
    const day_r = 1;
  const tenantQueries = constants.HORIZON_TENANT.map(horizonTenant => {
    return `
      (
        SELECT
          DATE(partitionTs, 'America/Sao_Paulo') AS data,
          horizonEnvironment AS plataforma,
          "total_pageviews" AS metrica,
          "${horizonTenant}" AS produto,
          COUNT(*) AS valor_metrica,
          current_datetime() AS data_ingestao
        FROM ${ctx.ref("common_hit_1_1*")}
        WHERE 
          DATE(partitionTs, 'America/Sao_Paulo') = DATE(DATE_SUB(current_date('America/Sao_Paulo'), INTERVAL ${day_r} DAY)) 
          AND tenant = IF("${horizonTenant}" = "home_globo", "home-globo", "${horizonTenant}")
        GROUP BY ALL
      )
    `;
  });

  const finalQuery = `
    SELECT *
    FROM ${tenantQueries.join("\nUNION ALL\n")}
  `;

  return finalQuery;
});
