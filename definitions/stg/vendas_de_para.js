publish(`vendas_de_para`, {
    type: "table",
    schema: "monitoramento",
    tags: ["salesforce", "vendas", "web","de_para","stg"] 
}).query(ctx => {
    const tenantQueries = constants.SF_VENDAS
        .map(datasetSF => {
            let day_r = 1;




            const salesforce_query = `
SELECT  DISTINCT origem_id, origem
FROM ${ctx.ref(datasetSF, "vendas_sf")} sf
WHERE ts_venda = (select max(ts_venda)
                  FROM ${ctx.ref(datasetSF, "vendas_sf")} sf_aux
                  WHERE sf_aux.origem_id = sf.origem_id 
                  ) 
`;

            return salesforce_query;
        });

    const finalQuery = `
    ${tenantQueries.join("\nUNION ALL\n")}
  `;

    return finalQuery;
});



