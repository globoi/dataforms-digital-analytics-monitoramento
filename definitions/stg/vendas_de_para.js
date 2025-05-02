publish(`vendas_de_para`, {
    type: "table",
    schema: "monitoramento",
    tags: ["salesforce", "vendas", "web","de_para","stg"] 
}).query(ctx => {
    const tenantQueries = constants.SF_VENDAS
        .map(datasetSF => {
            let day_r = 1;


            const salesforce_query = `
SELECT DISTINCT origem_id, origem
FROM ${ctx.ref(datasetSF, "vendas_sf")}
`;

            return salesforce_query;
        });

    const finalQuery = `
    ${tenantQueries.join("\nUNION ALL\n")}
  `;

    return finalQuery;
});
