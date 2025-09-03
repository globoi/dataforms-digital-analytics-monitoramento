constants.GA4_TENANT.forEach((tenant) => {
  let temp_tenant = ""
  if (tenant == "globoplay") {
    temp_tenant = "analytics_153317366";
  } else if (tenant == "g1") {
    temp_tenant = "analytics_153398069";
  } else if (tenant == "gshow") {
    temp_tenant = "analytics_153404207";
  } else if (tenant == "receitas") {
    temp_tenant = "analytics_290405147";
  } else if (tenant == "gigagloob") {
    temp_tenant = "analytics_309670617";
  } else if (tenant == "ge") {
    temp_tenant = "analytics_153402505";
  } else if (tenant == "home_globo") {
    temp_tenant = "analytics_290417375";
  }else if (tenant == "vitrine") {
    temp_tenant = "analytics_329311935";
  } else if (tenant == "cartola") {
    temp_tenant = "analytics_153734558";
  }
  const externalTable = {
    type: "declaration",
    database: 'gglobo-ga4-hdg-prd',
    schema: temp_tenant,
    name: 'events_*',
  }
  declare(externalTable)
  //let schema ga4 = if(tenant === "globoplay","analytics_153317366")

})
