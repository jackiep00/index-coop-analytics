WITH transfers AS (

  SELECT
    tr."from" AS address,
    -tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'transfer' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

  UNION ALL

  SELECT
    tr."to" AS address,
    tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'transfer' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

)
, balancer_add AS (

  SELECT
    tr."from" AS address,
    tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'balancer_add' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
    AND evt_tx_hash IN (
    
        SELECT
            evt_tx_hash
        FROM balancer."BPool_evt_LOG_JOIN"
        WHERE "tokenIn" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
        
    )

)
, balancer_remove AS (

  SELECT
    tr."to" AS address,
    -tr.value / 1e18 AS amount,
    date_trunc('day', evt_block_time) AS evt_block_day,
    'balancer_remove' AS type,
    evt_tx_hash
  FROM erc20."ERC20_evt_Transfer" tr
  WHERE contract_address = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
    AND evt_tx_hash IN (
    
        SELECT
            evt_tx_hash
        FROM balancer."BPool_evt_LOG_EXIT"
        WHERE "tokenOut" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
        
    )
    
),

uniswap_add AS (

  SELECT
    "to" AS address,
    ("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_add' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_addLiquidityETH"
  WHERE token = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN ("output_amountA"/1e18)
      WHEN "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN ("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_add' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router01_call_addLiquidity"
  WHERE "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
    OR "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN ("output_amountA"/1e18)
      WHEN "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN ("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_add' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_addLiquidity"
  WHERE "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
    OR "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

)
, uniswap_remove AS (

  SELECT
    "to" AS address,
    -("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidityETHWithPermit"
  WHERE token = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

  UNION ALL

  SELECT
    "to" AS address,
    -("output_amountToken"/1e18) AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidityETH"
  WHERE token = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN -("output_amountA"/1e18)
      WHEN "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN -("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router01_call_removeLiquidity"
  WHERE "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
    OR "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN -("output_amountA"/1e18)
      WHEN "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN -("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidity"
  WHERE "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
    OR "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

  UNION ALL

  SELECT
    "to" AS address,
    CASE
      WHEN "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN -("output_amountA"/1e18)
      WHEN "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd' THEN -("output_amountB"/1e18)
      ELSE 0
    END AS amount,
    date_trunc('day', call_block_time) AS evt_block_day,
    'uniswap_remove' AS type,
    call_tx_hash AS evt_tx_hash
  FROM uniswap_v2."Router02_call_removeLiquidityWithPermit"
  WHERE "tokenA" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'
    OR "tokenB" = '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'

)
, uniswapv3_add as (
  SELECT
    "from" as address,
    amount0 / 1e18 as amount,
    date_trunc('day', block_time) AS evt_block_day,
    'uniswapv3_add' as type,
    hash as evt_tx_hash
	
	FROM uniswap_v3."Pair_evt_Mint" m
	LEFT JOIN ethereum."transactions" tx ON m.evt_tx_hash = tx.hash
	WHERE tx.block_time > '5/4/21'
	and contract_address = '\x151ccb92bc1ed5c6d0f9adb5cec4763ceb66ac7f'
	
)
, uniswapv3_remove as (

  SELECT
    "from" as address,
    -amount0 / 1e18 as amount,
    date_trunc('day', block_time) AS evt_block_day,
    'uniswapv3_add' as type,
    hash as evt_tx_hash
    
  FROM uniswap_v3."Pair_evt_Burn" m
  LEFT JOIN ethereum."transactions" tx ON m.evt_tx_hash = tx.hash
  WHERE tx.block_time > '5/4/21'
  and contract_address = '\x151ccb92bc1ed5c6d0f9adb5cec4763ceb66ac7f'
	
)
, lp AS (
  SELECT * FROM uniswap_add
  UNION ALL
  SELECT * FROM uniswap_remove
  UNION ALL
  SELECT * FROM balancer_add
  UNION ALL
  SELECT * FROM balancer_remove
  union all
  select * from uniswapv3_add
  union all
  select * from uniswapv3_remove
)
, contracts AS (

  SELECT
    address,
    "type"
  FROM labels.labels
  WHERE "type" = 'contract_name'

)
, liquidity_providing AS (

  SELECT
    l.*,
    CASE c.type
      WHEN 'contract_name' THEN 'contract'
      ELSE 'non-contract'
    END AS contract
  FROM lp l
  LEFT JOIN contracts c ON l.address = c.address

)
, moves AS (

  SELECT
    *
  FROM transfers

  UNION ALL

  SELECT
    address,
    amount,
    evt_block_day,
    type,
    evt_tx_hash
  FROM liquidity_providing
  WHERE contract = 'non-contract'

)
, month_level_activity as (
  select address
      , date_trunc('month', evt_block_day) as txn_month
      , sum(amount) as net_monthly_change
      , count(*) as total_txns
      , sum(case when amount > 0 then amount else 0 end) as inflow
      , count(case when amount > 0 then amount else null end) as inflow_txns
      , sum(case when amount < 0 then amount else 0 end) as outflow
      , count(case when amount < 0 then amount else null end) as outflow_txns
  from moves
  where address <> '\x0000000000000000000000000000000000000000'
  group by 1,2
)
, month_series as (
  select generate_series(min(txn_month), date_trunc('month',now()), '1 month') as month
    from month_level_activity
)
, cohort_months as (
  select address
    , min(txn_month) as cohort_month
  from month_level_activity
  group by 1
)
-- get the rolling balances on a monthly basis
, monthly_exposure as (
  select m.month
    , address
    , sum(net_monthly_change) as exposure
  from month_series m
  left join month_level_activity a on m.month >= a.txn_month
  group by 1, 2
)
, user_analytics_raw as (
  select e.month
    , e.address
    , c.cohort_month
    , e.exposure
    , coalesce(mla.net_monthly_change,0) as net_monthly_change
    , coalesce(mla.total_txns,0) as total_txns
    , coalesce(mla.inflow,0) as inflow
    , coalesce(mla.inflow_txns,0) as inflow_txns
    , coalesce(mla.outflow,0) as outflow
    , coalesce(mla.outflow_txns,0) as outflow_txns
  from monthly_exposure e
  inner join cohort_months c on e.address = c.address
  left join month_level_activity mla 
    on e.address = mla.address
    and e.month = mla.txn_month
)
select case when (inflow <> 0 or outflow <> 0) then 'Active' else 'Inactive' end as activity
  , lag(exposure, 1) over (partition by address order by month)
  , case when lag(exposure, 1) over (partition by address order by month) is null then 'New Activity'
    when exposure < 0.0001 then 'No Exposure' -- leave threshold for dust
    when exposure >= 0.0001 and lag(exposure, 1) over (partition by address order by month) < 0.0001 then 'Resurrected Exposure'
    when exposure >= 0.0001 then 'Retained Exposure'
    else 'Error'
    end as exposure_type
  , *
from user_analytics_raw