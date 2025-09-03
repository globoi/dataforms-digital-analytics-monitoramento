publish(`salesforce`, {
    type: "incremental",
    schema: "monitoramento",
    tags: ["salesforce", "vendas", "web", "stg"]
}).query(ctx => {
    const tenantQueries = constants.SF_VENDAS
        .map(datasetSF => {
            let day_r = 1;
            const salesforce_query = `
                                        SELECT
                                            dt_venda data,
                                            canal_compra plataforma,
                                            "vendas" metrica,
                                            nome_produto produto,
                                            count(*) valor_metrica,
                                            current_datetime data_ingestao
                                        FROM ${ctx.ref(datasetSF, "vendas_sf")}
                                        WHERE dt_venda = DATE(DATE_SUB(current_date('America/Sao_Paulo'), INTERVAL ${day_r} DAY)) AND canal_compra = 'WEB'
                                        GROUP BY ALL

                                        UNION ALL 

                                        SELECT 
                                            dt_venda data, 
                                            canal_compra plataforma,
                                            "vendas_origem" metrica,  
                                            origem produto, 
                                            count(*) valor_metrica,
                                            current_datetime data_ingestao
                                            FROM ${ctx.ref(datasetSF, "vendas_sf")}
                                        WHERE dt_venda = DATE(DATE_SUB(current_date('America/Sao_Paulo'), INTERVAL ${day_r} DAY)) AND canal_compra = 'WEB'
                                        GROUP BY ALL
                                        `;
            return salesforce_query;
        });

    const finalQuery = `
    ${tenantQueries.join("\nUNION ALL\n")}
  `;

    return finalQuery;
});
