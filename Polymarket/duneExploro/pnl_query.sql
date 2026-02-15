-- Parses Polymarket logs to derive the P&L for all users and buckets them. P&L  = sum(sales + redemptions) - sum(buys)
-- https://dune.com/queries/6696909
SELECT
    CASE 
        -- NEGATIVE SIDE (Losses)
        WHEN balance < -100000 THEN 'a. Loss < -$100k'
        WHEN balance <= -10000 THEN 'b. Loss -$100k to -$10k'
        WHEN balance <= -1000  THEN 'c. Loss -$10k to -$1k'
        WHEN balance <= -750   THEN 'd. Loss -$1k to -$750'
        WHEN balance <= -500   THEN 'e. Loss -$750 to -$500'
        WHEN balance <= -250   THEN 'f. Loss -$500 to -$250'
        WHEN balance <= 0      THEN 'g. Loss -$250 to $0'
        
        -- POSITIVE SIDE (Profits)
        WHEN balance <= 250   THEN 'h. Profit $0 to $250'
        WHEN balance <= 500   THEN 'i. Profit $250 to $500'
        WHEN balance <= 750   THEN 'j. Profit $500 to $750'
        WHEN balance <= 1000   THEN 'k. Profit $750 to $1k'
        WHEN balance <= 10000  THEN 'l. Profit $1k to $10k'
        WHEN balance <= 100000 THEN 'm. Profit $10k to $100k'
        ELSE                        'n. Profit > $100k'
    END AS profit_bucket,
    COUNT(*) AS users_count,
    SUM(balance) AS total_pnl_in_bucket
FROM (
    SELECT
        user,
        SUM(CASE WHEN action = 'Buy' THEN amount ELSE 0 END) AS total_buy,
        SUM(CASE WHEN action = 'Sell' THEN amount ELSE 0 END) AS total_sell,
        SUM(CASE WHEN action = 'Redeem' THEN amount ELSE 0 END) AS total_redeem,
        SUM(CASE WHEN action = 'Sell' THEN amount ELSE 0 END) 
            + SUM(CASE WHEN action = 'Redeem' THEN amount ELSE 0 END)
            - SUM(CASE WHEN action = 'Buy' THEN amount ELSE 0 END) AS balance
    FROM (
        -- 1. REDEMPTIONS
        SELECT
            '0x' || SUBSTRING(LOWER(TRY_CAST(topic1 AS VARCHAR)), 27, 40) AS user,
            payout / 1e6 AS amount,
            'Redeem' AS action
        FROM
            TABLE (
                decode_evm_event ( -- https://docs.dune.com/query-engine/Functions-and-operators/evm-decoding-functions
                    abi => '{ "anonymous": false, "inputs": [ { "indexed": true, "name": "redeemer", "type": "address" }, { "indexed": true, "name": "collateralToken", "type": "address" }, { "indexed": true, "name": "parentCollectionId", "type": "bytes32" }, { "indexed": false, "name": "conditionId", "type": "bytes32" }, { "indexed": false, "name": "indexSets", "type": "uint256[]" }, { "indexed": false, "name": "payout", "type": "uint256" } ], "name": "PayoutRedemption", "type": "event" }',
                    input => TABLE (
                        SELECT DISTINCT *
                        FROM polygon.logs -- https://docs.dune.com/data-catalog/evm/polygon/raw/logs
                        WHERE topic0 = 0x2682012a4a4f1973119f1c9b90745d1bd91fa2bab387344f044cb3586864d18d
                    )
                )
            )
        WHERE contract_address = 0x4D97DCd97eC945f40cF65F87097ACe5EA0476045 -- https://polygonscan.com/address/0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
        
        UNION ALL
        
        -- 2. TRADES (BUYS & SELLS)
        SELECT
            '0x' || SUBSTRING(LOWER(TRY_CAST(topic2 AS VARCHAR)), 27, 40) AS user,
            CASE
                WHEN makerAssetId = 0 THEN makerAmountFilled / 1e6       
                WHEN takerAssetId = 0 THEN takerAmountFilled / 1e6
            END AS amount,                                   
            CASE
                WHEN makerAssetId = 0 THEN 'Buy'
                WHEN takerAssetId = 0 THEN 'Sell'
            END AS action
        FROM
            TABLE (
                decode_evm_event (
                    abi => '{ "anonymous": false, "inputs": [ { "indexed": true, "internalType": "bytes32", "name": "orderHash", "type": "bytes32" }, { "indexed": true, "internalType": "address", "name": "maker", "type": "address" }, { "indexed": true, "internalType": "address", "name": "taker", "type": "address" }, { "indexed": false, "internalType": "uint256", "name": "makerAssetId", "type": "uint256" }, { "indexed": false, "internalType": "uint256", "name": "takerAssetId", "type": "uint256" }, { "indexed": false, "internalType": "uint256", "name": "makerAmountFilled", "type": "uint256" }, { "indexed": false, "internalType": "uint256", "name": "takerAmountFilled", "type": "uint256" }, { "indexed": false, "internalType": "uint256", "name": "fee", "type": "uint256" } ], "name": "OrderFilled", "type": "event" }',
                    input => TABLE (
                        SELECT *
                        FROM polygon.logs -- https://docs.dune.com/data-catalog/evm/polygon/raw/logs
                        WHERE topic0 = 0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6
                    )
                )
            )
    ) combined
    GROUP BY user
    HAVING SUM(CASE WHEN action = 'Buy' THEN amount ELSE 0 END) > 0 
) aggregated
GROUP BY 1
ORDER BY 1 ASC;