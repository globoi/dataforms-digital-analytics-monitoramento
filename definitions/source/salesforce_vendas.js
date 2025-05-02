constants.SF_VENDAS.forEach((tenant) => {
  
  const externalTable = {
    type: "declaration",
    database: 'gglobo-mkt-ins-hdg-prd',
    schema: tenant,
    name: 'vendas_sf',
  }
  declare(externalTable)

})
