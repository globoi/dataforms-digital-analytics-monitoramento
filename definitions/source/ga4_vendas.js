constants.GA4_VENDAS.forEach((tenant) => {
  
  const externalTable = {
    type: "declaration",
    database: 'gglobo-ga4-hdg-prd',
    schema: tenant,
    name: 'events_*',
  }
  declare(externalTable)

})
