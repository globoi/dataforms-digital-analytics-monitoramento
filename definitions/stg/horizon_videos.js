publish(`horizon_videos`, {
  type: "incremental",
  schema: "monitoramento",
  tags: ["horizon", "videos", "video_views", "video_playtime","stg"]
}).query(ctx => {
  const day_r = 1;

  return `
WITH tab_player_session_info AS (
  SELECT 
    DATE(ev.partitionTs, "America/Sao_Paulo") AS dia,
    IF(tenant = 'player', 'globoplay', tenant) AS tenant,
    videoSessionId,
    MAX(horizonenvironment) AS horizonenvironment,
    MAX(horizonClientDeviceGroup) AS horizonClientDeviceGroup,
    MAX(horizonClientType) AS horizonClientType,
    MAX(IF(kind='live',1,0)) AS is_live
  FROM ${ctx.ref("player_session_info_*")} ev
  WHERE DATE(ev.partitionTs, "America/Sao_Paulo") = DATE(DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL ${day_r} DAY))
    AND tenant IN ('player', 'globoplay', 'g1', 'ge', 'gshow', 'receitas', 'cartola', 'home_globo', 'receitas', 'vitrine')
  GROUP BY dia, tenant, videoSessionId
),

    tab_vdi_vs AS (
      SELECT DISTINCT 
        partitionDate AS dia,
        IF(tenant = 'player', 'globoplay', tenant) AS tenant,
        playertype, 
        videoSessionId AS videoSessionId,
        userid, 
        provider,
        totalPlayingTime played
      FROM ${ctx.ref("on_demand")} ev
      WHERE partitionDate= DATE(DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL ${day_r} DAY))  
        AND totalPlayingTime > 0
        AND tenant IN ('player', 'globoplay', 'g1', 'ge', 'gshow', 'receitas', 'cartola', 'home_globo', 'receitas', 'vitrine')
    ),

    tab_vdi_vls AS (
      SELECT DISTINCT 
        partitionDate AS dia,
        IF(tenant = 'player', 'globoplay', tenant) AS tenant,
        playertype,
        videoSessionId AS videoSessionId,
        totalPlayingTime played
      FROM ${ctx.ref("live")} ev
      WHERE partitionDate = DATE(DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL ${day_r} DAY))  
        AND totalPlayingTime > 0
        AND tenant IN ('player', 'globoplay', 'g1', 'ge', 'gshow', 'receitas', 'cartola', 'home_globo', 'receitas', 'vitrine')
    ),

    union_tabs AS (
      SELECT 
        'horizon' AS fonte,
        dia,
        'vs' AS tipo_video,
        tenant,
        videoSessionId,
        horizonenvironment,
        horizonClientType,
        horizonClientDeviceGroup,
        playertype,
        COUNT(*) AS videoviews,
        SUM(played) AS played_total
      FROM tab_vdi_vs
      INNER JOIN tab_player_session_info USING(dia, tenant, videoSessionId)
      GROUP BY dia, tenant, videoSessionId, horizonenvironment, horizonClientType, horizonClientDeviceGroup, playertype

      UNION ALL

      SELECT 
        'horizon' AS fonte,
        dia,
        'vls' AS tipo_video,
        tenant,
        videoSessionId,
        horizonenvironment,
        horizonClientType,
        horizonClientDeviceGroup,
        playertype,
        COUNT(*) AS videoviews,
        SUM(played) AS played_total
      FROM tab_vdi_vls
      INNER JOIN tab_player_session_info USING(dia, tenant, videoSessionId)
      GROUP BY dia, tenant, videoSessionId, horizonenvironment, horizonClientType, horizonClientDeviceGroup, playertype
    ),

    tab_vdi AS (
      SELECT 
        dia, 
        tenant, 
        CASE
          WHEN horizonClientDeviceGroup LIKE '%tv%' AND REGEXP_CONTAINS(horizonClientType, 'android|fire') THEN 'tv-android' 
          WHEN horizonClientDeviceGroup LIKE '%tv%' AND NOT REGEXP_CONTAINS(horizonClientType, 'android|fire') THEN 'tv_smart_e_outras' 
          WHEN horizonClientType LIKE '%roku%' OR playertype LIKE '%roku%' THEN 'tv-roku' 
          WHEN horizonenvironment = 'app' AND playertype LIKE '%roku%' THEN 'tv_smart_e_outras' 
          WHEN horizonenvironment LIKE '%app%' AND horizonClientDeviceGroup = 'mobile' AND horizonClientType = 'android' THEN 'app-mobile-android'    
          WHEN horizonenvironment LIKE '%app%' AND horizonClientDeviceGroup = 'mobile' AND horizonClientType = 'iOS' THEN 'app-mobile-ios'    
          WHEN horizonenvironment LIKE '%web%' AND horizonClientDeviceGroup NOT LIKE '%tv%' AND REGEXP_CONTAINS(playertype ,'android|safari_mobile|safari') THEN 'web-mobile'    
          WHEN horizonenvironment LIKE '%web%' AND playertype LIKE '%desktop%' THEN 'web-desktop'    
          ELSE 'outros' 
        END AS platform,
        SUM(videoviews) AS videoviews,
        SUM(played_total)/1000 AS played_total
      FROM union_tabs
      GROUP BY dia, tenant, platform
    )

    SELECT dia data,
           platform plataforma,
           "video_views" metrica,
           tenant produto,
           videoviews valor_metrica,   
           current_datetime() AS data_ingestao          
    FROM tab_vdi

    UNION ALL

        SELECT dia data,
           platform plataforma,
           "video_playtime" metrica,
           tenant produto,
           cast(played_total as int64) valor_metrica,   
           current_datetime() AS data_ingestao          
    FROM tab_vdi  `;
});
