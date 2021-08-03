/*
    query here: https://duneanalytics.com/queries/54043

    forked from https://duneanalytics.com/queries/22041/46378

    --- INDEX Treasury ---

    Wallet / Address
    ('\x9467cfadc9de245010df95ec6a585a506a8ad5fc', -- Treasury Wallet
    '\xe2250424378b6a6dC912f5714cfd308a8D593986', -- Treasury Committee Wallet
    '\x26e316f5b3819264DF013Ccf47989Fb8C891b088' -- Community Treasury Year 1 Vesting
    )    
    

*/

-- Start Generalized Price Feed block - see generalized_price_feed.sql

WITH prices_usd AS (

    SELECT
        date_trunc('day', minute) AS dt
        , symbol
        , decimals
        , AVG(price) AS price
    FROM prices.usd
    WHERE symbol in ('INDEX', 'DPI', 'MVI', 'ETH2x-FLI', 'BTC2x-FLI', 'USDC')
    GROUP BY 1,2,3
)
    
, eth_swaps AS (
    -- Uniswap price feed
    SELECT
        date_trunc('hour', sw."evt_block_time") AS hour
        , case 
            when contract_address = '\x3452A7f30A712e415a0674C0341d44eE9D9786F9' then 'INDEX'
            when contract_address = '\x4d5ef58aac27d99935e5b6b4a6778ff292059991' then 'DPI'
            when contract_address = '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff' then 'MVI'
            when contract_address = '\xf91c12dae1313d0be5d7a27aa559b1171cc1eac5' then 'ETH2x-FLI'
        end as symbol
        , ("amount0In" + "amount0Out")/1e18 AS a0_amt
        , ("amount1In" + "amount1Out")/1e18 AS a1_amt
    FROM uniswap_v2."Pair_evt_Swap" sw
    WHERE contract_address in ( '\x3452A7f30A712e415a0674C0341d44eE9D9786F9' -- liq pair addresses I am searching the price for
                                , '\x4d5ef58aac27d99935e5b6b4a6778ff292059991'
                                , '\x4d3C5dB2C68f6859e0Cd05D080979f597DD64bff' 
                                , '\xf91c12dae1313d0be5d7a27aa559b1171cc1eac5' )
        AND sw.evt_block_time >= '2020-09-10'
)
, btc_swaps as (   
    -- Sushi price feed
    SELECT
        date_trunc('hour', sw."evt_block_time") AS hour
        , 'BTC2x-FLI' as symbol
        , ("amount0In" + "amount0Out")/1e18 AS a0_amt
        , ("amount1In" + "amount1Out")/1e8 AS a1_amt
    FROM sushi."Pair_evt_Swap" sw
    WHERE contract_address = '\x164fe0239d703379bddde3c80e4d4800a1cd452b' -- liq pair address I am searching the price for
        AND sw.evt_block_time >= '2021-05-11'

)

, swap_a1_eth_prcs AS (

    SELECT 
        avg(price) a1_prc
        , date_trunc('hour', minute) AS hour
    FROM prices.usd
    WHERE minute >= '2020-09-10'
        AND contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --weth as base asset
    GROUP BY 2                
)

, swap_a1_btc_prcs as (

    SELECT 
        avg(price) a1_prc, 
        date_trunc('hour', minute) AS hour
    FROM prices.usd
    WHERE minute >= '2021-05-11'
        AND contract_address ='\x2260fac5e5542a773aa44fbcfedf7c193bc2c599' --wbtc as base asset
    GROUP BY 2
)

, swap_hours AS (
    
    SELECT generate_series('2020-09-10 00:00:00'::timestamp, date_trunc('hour', NOW()), '1 hour') AS hour -- Generate all days since the first contract
    
)
, eth_temp AS (

    SELECT
        h.hour
        , s.symbol
        , COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price
        -- , COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as asset_price
        -- a1_prcs."minute" AS minute
    FROM swap_hours h
    LEFT JOIN eth_swaps s ON h."hour" = s.hour 
    LEFT JOIN swap_a1_eth_prcs a ON h."hour" = a."hour"
    GROUP BY 1,2

) 
, btc_temp as (
    SELECT
        h.hour
        , s.symbol
        , COALESCE(AVG((s.a1_amt/s.a0_amt)*a.a1_prc), NULL) AS usd_price
        -- , COALESCE(AVG(s.a1_amt/s.a0_amt), NULL) as asset_price
        -- a1_prcs."minute" AS minute
    FROM swap_hours h
    LEFT JOIN btc_swaps s ON h."hour" = s.hour 
    LEFT JOIN swap_a1_btc_prcs a ON h."hour" = a."hour"
    GROUP BY 1,2
)
, swap_temp as (
    select * from eth_temp
    union
    select * from btc_temp
)
, swap_feed AS (
    SELECT
        hour
        , symbol
        , (ARRAY_REMOVE(ARRAY_AGG(usd_price) OVER (PARTITION BY symbol ORDER BY hour), NULL))[COUNT(usd_price) OVER (PARTITION BY symbol ORDER BY hour)] AS usd_price
        -- , (ARRAY_REMOVE(ARRAY_AGG(asset_price) OVER (PARTITION BY symbol ORDER BY hour), NULL))[COUNT(asset_price) OVER (PARTITION BY symbol ORDER BY hour)] AS asset_price
    FROM swap_temp
)
, swap_price_feed AS ( -- only include the uni feed when there's no corresponding price in prices_usd

    SELECT
        date_trunc('day', hour) AS dt
        , u.symbol
        , AVG(usd_price) AS price
    FROM swap_feed u
    left join prices_usd p on date_trunc('day', u.hour) = p.dt
        and u.symbol = p.symbol
    WHERE p.dt is null
        AND usd_price IS NOT NULL
    GROUP BY 1, 2

),

prices AS (

SELECT
    *
FROM prices_usd

UNION ALL

SELECT dt  
    , symbol
    , 18 as decimals -- all the INDEX tokens have 18 decimals
    , price
FROM swap_price_feed

)
-- End price feed block - output is CTE "prices"
, wallets AS (
    select '\x154c154c589b4aeccbf186fb8bc668cd7c213762'::bytea as address
        , 'Centralised Exchange Listing' as address_alias
    union all
    select '\xe83de75eb3e84f3cbca3576351d81dbeda5645d4'::bytea as address
        , 'Analytics Working Group' as address_alias
    union all
    select '\xd4bcc2b5d21fe67c8be351cdb47ec1b2cd7e84a7'::bytea as address
        , 'Growth Working Group' as address_alias
    union all
    select '\x0dea6d942a2d8f594844f973366859616dd5ea50'::bytea as address
        , 'DPI Manager' as address_alias
    union all
    select '\x25100726b25a6ddb8f8e68988272e1883733966e'::bytea as address
        , 'DPI Rebalancer' as address_alias
    union all
    select '\xaa6e8127831c9de45ae56bb1b0d4d4da6e5665bd'::bytea as address
        , 'ETH2x-FLI Token' as address_alias
    union all
    select '\x445307De5279cD4B1BcBf38853f81b190A806075'::bytea as address
        , 'ETH2x-FLI Manager' as address_alias
    union all
    select '\x1335D01a4B572C37f800f45D9a4b36A53a898a9b'::bytea as address
        , 'ETH2x-FLI Strategy Adapter' as address_alias
    union all
    select '\x26F81381018543eCa9353bd081387F68fAE15CeD'::bytea as address
        , 'ETH2x-FLI Fee Adapter' as address_alias
    union all
    select '\x0F1171C24B06ADed18d2d23178019A3B256401D3'::bytea as address
        , 'ETH2x-FLI SupplyCapIssuanceHook' as address_alias
    union all
    select '\x0b498ff89709d3838a063f1dfa463091f9801c2b'::bytea as address
        , 'BTC2x-FLI Token' as address_alias
    union all
    select '\xC7Aede3B12daad3ffa48fc96CCB65659fF8D261a'::bytea as address
        , 'BTC2x-FLI Manager' as address_alias
    union all
    select '\x4a99733458349505A6FCbcF6CD0a0eD18666586A'::bytea as address
        , 'BTC2x-FLI Strategy Adapter' as address_alias
    union all
    select '\xA0D95095577ecDd23C8b4c9eD0421dAc3c1DaF87'::bytea as address
        , 'BTC2x-FLI Fee Adapter' as address_alias
    union all
    select '\x6c8137f2f552f569cc43bc4642afbe052a12441c'::bytea as address
        , 'BTC2x-FLI SupplyCapAllowedCallerIssuanceHook' as address_alias
    union all
    select '\x0954906da0Bf32d5479e25f46056d22f08464cab'::bytea as address
        , 'INDEX Token Address' as address_alias
    union all
    select '\xDD111F0fc07F4D89ED6ff96DBAB19a61450b8435'::bytea as address
        , 'INDEX Initial Airdrop Address' as address_alias
    union all
    select '\x8f06FBA4684B5E0988F215a47775Bb611Af0F986'::bytea as address
        , 'INDEX DPI Farming Contract 1 (Oct - Dec)' as address_alias
    union all
    select '\xB93b505Ed567982E2b6756177ddD23ab5745f309'::bytea as address
        , 'INDEX DPI Farming Contract 2 (Dec. 2020 - March 2021)' as address_alias
    union all
    select '\x66a7d781828B03Ee1Ae678Cd3Fe2D595ba3B6000'::bytea as address
        , 'Index Methodologist Bounty (18 months vesting)' as address_alias
    union all
    select '\x26e316f5b3819264DF013Ccf47989Fb8C891b088'::bytea as address
        , 'Community Treasury Year 1 Vesting' as address_alias
    union all
    select '\xd89C642e52bD9c72bCC0778bCf4dE307cc48e75A'::bytea as address
        , 'Community Treasury Year 2 Vesting' as address_alias
    union all
    select '\x71F2b246F270c6AF49e2e514cA9F362B491Fbbe1'::bytea as address
        , 'Community Treasury Year 3 Vesting' as address_alias
    union all
    select '\xf64d061106054Fe63B0Aca68916266182E77e9bc'::bytea as address
        , 'Set Labs Year 1 Vesting' as address_alias
    -- union all
    -- select NULL as address -- need to look this up - on the website the address is invalid
    --     , 'Set Labs Year 2 Vesting' as address_alias
    union all
    select '\x0D627ca04A97219F182DaB0Dc2a23FB4a5B02A9D'::bytea as address
        , 'Set Labs Year 3 Vesting' as address_alias
    union all
    select '\x0D627ca04A97219F182DaB0Dc2a23FB4a5B02A9D'::bytea as address
        , 'Set Labs Year 3 Vesting' as address_alias
    union all
    select '\x319b852cd28b1cbeb029a3017e787b98e62fd4e2'::bytea as address
        , 'Rewards Merkle Distributor / January 2021 Merkle Rewards Account' as address_alias
    union all
    select '\xeb1cbc809b21dddc71f0f9edc234eee6fb29acee'::bytea as address
        , 'December 2020 Merkle Rewards Account' as address_alias
    union all
    select '\x209f012602669c88bbda687fbbfe6a0d67477a5d'::bytea as address
        , 	'October 2020 Merkle Rewards Account' as address_alias
    union all
    select '\xa6bb7b6b2c5c3477f20686b98ea09796f8f93184'::bytea as address
        ,	'November 2020 Merkle Rewards Account' as address_alias
    union all
    select '\xCa3C3570beb35E5d3D85BCd8ad8F88BefaccFF10'::bytea as address
        , 'February 2021 Merkle Rewards Account' as address_alias
    union all
    select '\xa87fbb413f8de11e47037c5d697cc03de29e4e4b'::bytea as address
        , 'March 2021 Merkle Rewards Account' as address_alias
    union all
    select '\x973a526a633313b2d32b9a96ed16e212303d6905'::bytea as address
        ,	'April 2021 Merkle Rewards Account' as address_alias
    union all
    select '\x10F87409E405c5e44e581A4C3F2eECF36AAf1f92'::bytea as address
        , 'INDEX Sale 2 of 3 Multisig - Dylan, Greg, Punia' as address_alias
)

, creation_days AS (
    SELECT
        date_trunc('day', block_time) AS day
    FROM ethereum.traces
    WHERE address IN (SELECT address FROM wallets)
    AND TYPE = 'create'
)
, days AS (
    SELECT 
        generate_series(MIN(day), date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
    FROM creation_days
)
, transfers AS (
    --ERC20 Tokens
    SELECT
        date_trunc('day', evt_block_time) AS day,
        "from" AS address,
        contract_address,
        sum(-value) AS amount
    FROM erc20."ERC20_evt_Transfer"
    WHERE "from" IN (SELECT address FROM wallets)
    AND evt_block_time >= (SELECT min(day) FROM creation_days)
    GROUP BY 1,2,3
    
    UNION ALL

    SELECT
        date_trunc('day', evt_block_time) AS day,
        "to" AS address,
        contract_address,
        sum(value) AS amount
    FROM erc20."ERC20_evt_Transfer"
    WHERE "to" IN (SELECT address FROM wallets)
    AND evt_block_time >= (SELECT min(day) FROM creation_days)
    GROUP BY 1,2,3
)

, transfers_day AS (
    SELECT
        t.day,
        t.address,
        t.contract_address,
        sum(t.amount/10^18) AS change -- all target contracts have decimals of 18
    FROM transfers t
    GROUP BY 1,2,3
)

, balances_w_gap_days AS (
    SELECT
        day,
        address,
        contract_address,
        sum(change) OVER (PARTITION BY address, contract_address ORDER BY day) AS "balance",
        lead(day, 1, now()) OVER (PARTITION BY address, contract_address ORDER BY day) AS next_day
    FROM transfers_day
)

, balances_all_days AS (
    SELECT
        d.day,
        b.address,
        b.contract_address,
        sum(b.balance) AS "balance"
    FROM balances_w_gap_days b
    INNER JOIN days d ON b.day <= d.day AND d.day < b.next_day
    GROUP BY 1,2,3
    ORDER BY 1,2,3
)
, usd_value_all_days as (
    SELECT
        b.day,
    --    b.address,
        w.address_alias,
    --    w.org,
        b.contract_address,
        p.symbol AS token,
        b.balance,
        p.price,
        b.balance * coalesce(p.price,0) AS usd_value
        , rank() over (order by b.day desc)
    FROM balances_all_days b
    left join erc20.tokens t on b.contract_address = t.contract_address
    inner JOIN prices p ON t.symbol = p.symbol AND b.day = p.dt
    LEFT OUTER JOIN wallets w ON b.address = w.address
    where b.day <= '{{end_date}}'
    ORDER BY usd_value DESC
    LIMIT 10000
)
select 
    address_alias
    , token
    , balance
    , usd_value
    , contract_address
from usd_value_all_days
where rank = 1
;

