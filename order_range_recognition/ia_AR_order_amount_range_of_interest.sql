-- Query: order_amount_range_of_interest.sql 
-- Analyzes the distribution of rejected order value amounts.
-- This advanced analytical query identifies the highest frequency bins in the histogram of rejected orders,
-- systematically selecting these bins until reaching a specified cumulative proportion (90%).
-- The implementation uses the Knuth (Bayesian) method with a two-stage filtering approach:
--   1. First applying logarithmic transformation to handle the wide range of financial values
--   2. Then applying a linear scale analysis on the filtered subset
-- Both stages use pre-aggregation techniques for computational efficiency.

-- =============================================================================
-- STAGE 1: DATA EXTRACTION AND PREPARATION
-- =============================================================================
-- BASE_DATA: Extract raw order data from the past 6 months with payment status information
-- Process: Join order table with processor acceptance rate table to get payment attempt outcomes
-- Output: Raw order data with payment status, transaction categorization, and metadata
WITH BASE_DATA AS (
    SELECT   
        o.ORDER_ID,
        o.ORDER_DATE,
        o.ORDER_TIME,
        o.COMMERCE_ID,
        DATE_TRUNC('month', o.ORDER_DATE) AS DATE_MONTH,
        o.ORDER_PAYMENT_METHOD AS PAYMENT_ATTEMPT_METHOD_TYPE,
        CASE 
            WHEN o.ORDER_TOTAL_AMOUNT_USD < 50 THEN 'SMALL'
            WHEN o.ORDER_TOTAL_AMOUNT_USD BETWEEN 50 AND 200 THEN 'MEDIUM'
            ELSE 'LARGE'
        END AS TRANSACTION_SIZE,
        o.ORDER_APPROVED_ACCEPTANCE_RATE_INDICATOR AS PAYMENT_APPROVED_INDICATOR,
        o.ORDER_ACCEPTANCE_RATE_INDICATOR AS PAYMENT_ATTEMPTED_INDICATOR,
        p.PAYMENT_ERROR_CATEGORY AS PAYMENT_ATTEMPT_ERROR_CATEGORY,
        o.ORDER_TOTAL_AMOUNT_USD AS PAYMENT_ATTEMPT_AMOUNT_USD,
        o.ORDER_CITY_NAME,
        o.CARD_COUNTRY,
        o.PAYMENT_ROUTING_STRATEGY,
        o.ORDER_PAYMENT_METHOD,
        o.LAST_CARD_BIN AS CARD_BIN
    FROM VW_ATHENA_ORDER_ o
    INNER JOIN (
        SELECT p1.COMMERCE_ID, p1.ORDER_ID, p1.PAYMENT_ERROR_CATEGORY, p1.PAYMENT_ERROR_CODE
        FROM VW_ATHENA_ACCEPTANCE_RATE_PROCESSOR_ p1
        INNER JOIN (
            SELECT COMMERCE_ID, ORDER_ID, MAX(PAYMENT_SEQUENCE_ORDER) AS MAX_SEQUENCE_ORDER
            FROM VW_ATHENA_ACCEPTANCE_RATE_PROCESSOR_
            GROUP BY COMMERCE_ID, ORDER_ID
        ) p2
          ON p1.COMMERCE_ID = p2.COMMERCE_ID 
         AND p1.ORDER_ID    = p2.ORDER_ID 
         AND p1.PAYMENT_SEQUENCE_ORDER = p2.MAX_SEQUENCE_ORDER
    ) p ON o.COMMERCE_ID = p.COMMERCE_ID AND o.ORDER_ID = p.ORDER_ID
    WHERE o.ORDER_DATE >= DATEADD(month, -6, CURRENT_DATE())
),
-- ORDER_TABLE: Transform raw data into a structured format with clearer column naming
-- Process: Clean and standardize column names, classify orders by payment approval status
-- Input: BASE_DATA (raw joined order and processor data)
-- Output: Standardized order data with clear payment status classification (APPROVED/DECLINED/OTHER)
ORDER_TABLE AS (
    SELECT 
        orders.ORDER_ID,
        orders.ORDER_DATE,
        orders.ORDER_TIME,
        orders.COMMERCE_ID,
        orders.DATE_MONTH,
        orders.PAYMENT_ATTEMPT_METHOD_TYPE AS PAYMENT_METHOD,
        orders.TRANSACTION_SIZE,
        orders.PAYMENT_ATTEMPT_AMOUNT_USD AS PAYMENT_AMOUNT_USD,
        orders.ORDER_CITY_NAME,
        orders.CARD_COUNTRY,
        orders.PAYMENT_ROUTING_STRATEGY,
        orders.ORDER_PAYMENT_METHOD,
        orders.CARD_BIN,
        CASE 
            WHEN orders.PAYMENT_APPROVED_INDICATOR = TRUE THEN 'APPROVED'
            WHEN orders.PAYMENT_ATTEMPTED_INDICATOR = TRUE AND orders.PAYMENT_APPROVED_INDICATOR = FALSE THEN 'DECLINED'
            ELSE 'OTHER'
        END AS ORDER_STATUS
    FROM BASE_DATA AS orders
),
-- REJECTED_TABLE: Filter to include only declined/rejected orders for histogram analysis
-- Process: Filter ORDER_TABLE to keep only declined payment attempts, add recency flag
-- Input: ORDER_TABLE (all orders with status classification)
-- Output: Subset containing only rejected orders with last month indicator for trend analysis
REJECTED_TABLE AS (
    SELECT *,
           CASE WHEN ORDER_DATE >= DATEADD(month, -1, CURRENT_DATE) THEN 1 ELSE 0 END AS is_last_month
    FROM ORDER_TABLE
    WHERE ORDER_STATUS = 'DECLINED'
),
-- =============================================================================
-- STAGE 2: FIRST-LEVEL KNUTH ANALYSIS ON LOGARITHMIC SCALE
-- =============================================================================
-- Apply logarithmic transformation to handle the wide range of financial values (outliers)
-- The Knuth (Bayesian) method determines optimal histogram bin count to identify top 90% most frequent ranges
/* ========= Knuth Method on Logarithmic Scale (with pre-aggregation) ========= */
-- This stage uses log-transformed data to better handle financial data's right-skewed distribution
-- The method finds optimal bin count M* that maximizes Bayesian evidence F(M)

-- PARAMS: Define algorithmic parameters for the first Knuth analysis
-- Process: Set constants for micro-bin count, bin count range, and geometric progression step
-- Output: Configuration parameters (U=20000 micro-bins, M range 64-4096, step 1.03)
PARAMS AS (
    SELECT 
        20000::INT AS U,               -- micro-bins in log space for pre-aggregation
        64::INT    AS M_MIN,           -- minimum bin count to evaluate
        4096::INT  AS M_MAX,           -- maximum bin count to evaluate
        1.03::FLOAT AS M_STEP          -- geometric step size for M grid exploration
),
-- LOG_STATS: Calculate basic statistics for log-transformed payment amounts
-- Process: Compute count, min, and max of natural logarithm of payment amounts from REJECTED_TABLE
-- Input: REJECTED_TABLE (rejected orders only)
-- Output: N (sample size), Y_MIN (minimum log value), Y_MAX (maximum log value) for histogram bounds
LOG_STATS AS (
    SELECT 
        COUNT(*)::INT AS N,
        MIN(LN(PAYMENT_AMOUNT_USD)) AS Y_MIN,
        MAX(LN(PAYMENT_AMOUNT_USD)) AS Y_MAX
    FROM REJECTED_TABLE
    WHERE PAYMENT_AMOUNT_USD > 0
),
-- MGRID: Generate a geometric grid of potential bin counts (M values) to evaluate
-- Process: Create sequence from M_MIN to M_MAX using geometric progression (M_STEP^t)
-- Input: PARAMS (algorithm parameters)
-- Output: Distinct integer M values from 64 to 4096 for Bayesian evaluation
MGRID AS (
    SELECT DISTINCT CAST(ROUND(m_val) AS INT) AS M
    FROM (
        SELECT P.M_MIN * POWER(P.M_STEP, SEQ4()) AS m_val
        FROM PARAMS P, TABLE(GENERATOR(ROWCOUNT=>300))
    )
    WHERE m_val BETWEEN (SELECT M_MIN FROM PARAMS) AND (SELECT M_MAX FROM PARAMS)
),
-- MICRO_BINS: Pre-aggregation step 1 - Assign each payment amount to one of U micro-bins
-- Process: Transform each log(payment_amount) to micro-bin index u using uniform division
-- Input: REJECTED_TABLE (payment amounts), LOG_STATS (log bounds), PARAMS (U=20000)
-- Output: Each rejected order mapped to micro-bin index u ∈ [0, U-1]
MICRO_BINS AS (
    SELECT 
        LEAST(GREATEST(
            -- Use CEIL instead of FLOOR to assign boundary values to the higher bin
            CAST(CEIL( (LN(T.PAYMENT_AMOUNT_USD) - S.Y_MIN) / ((S.Y_MAX - S.Y_MIN) / P.U) ) - 1 AS INT)
        , 0), P.U-1) AS u
    FROM REJECTED_TABLE T
    JOIN LOG_STATS S ON TRUE
    JOIN PARAMS P    ON TRUE
    WHERE T.PAYMENT_AMOUNT_USD > 0
),
-- MICRO_COUNTS: Count observations in each micro-bin
-- Process: Aggregate MICRO_BINS to count how many orders fall in each micro-bin u
-- Input: MICRO_BINS (order-to-micro-bin mappings)
-- Output: Frequency count c for each micro-bin u (pre-aggregated data for efficiency)
MICRO_COUNTS AS (
    SELECT u, COUNT(*)::INT AS c
    FROM MICRO_BINS
    GROUP BY u
),
-- MACRO_COUNTS: For each candidate M value, map micro-bins to macro-bins and aggregate counts
-- Process: For each M candidate, map micro-bin u to macro-bin k using k = floor(u*M/U), sum counts
-- Input: MICRO_COUNTS (micro-bin frequencies), MGRID (M candidates), PARAMS (U)
-- Output: For each (M,k) pair, the aggregated count n_k of orders in macro-bin k
MACRO_COUNTS AS (
    -- k = floor(u * M / U)
    SELECT 
        M.M,
        CAST(FLOOR( (MC.u * M.M) / P.U ) AS INT) AS k,
        SUM(MC.c)::INT AS n_k
    FROM MICRO_COUNTS MC
    CROSS JOIN MGRID M
    JOIN PARAMS P ON TRUE
    GROUP BY M.M, CAST(FLOOR( (MC.u * M.M) / P.U ) AS INT)
),

/* ---- Stirling's Approximation for lnGamma(x):
   ln Γ(x) ≈ (x-0.5) ln x - x + 0.5 ln(2π) + 1/(12x) - 1/(360x^3)
   (This approximation is sufficient for our M range; empty bins are handled separately)
*/
-- STIRLING_CONST: Calculate constants needed for the Stirling approximation of the gamma function
-- Process: Pre-compute mathematical constants for Stirling's approximation of ln(Γ(x))
-- Input: Mathematical constants (π)
-- Output: LN_2PI and LN_GAMMA_HALF_CONST for efficient gamma function approximation
STIRLING_CONST AS (
    SELECT 
      LN(2*PI()) AS LN_2PI,
      0.5*LN(PI()) AS LN_GAMMA_HALF_CONST  -- ln Γ(1/2) = 0.5 ln π (this is a known value)
),
-- TERMS: Calculate the sum of ln(Gamma(n_k+1/2)) terms for each candidate M
-- Process: For each M, sum Stirling approximation of ln(Γ(n_k + 1/2)) across all macro-bins
-- Input: MACRO_COUNTS (bin frequencies), STIRLING_CONST (gamma constants)
-- Output: For each M, the sum of log-gamma terms needed for Bayesian score F(M)
TERMS AS (
    SELECT 
      MC.M,
      SUM(
          /* lnGamma(n_k + 1/2) via Stirling, x = n_k + 0.5 */
          (
            (( (n_k + 0.5) - 0.5) * LN(n_k + 0.5)) -- (x-0.5) ln x
            - (n_k + 0.5)                          -- -x
            + 0.5 * (SELECT LN_2PI FROM STIRLING_CONST)  -- + 0.5 ln(2π)
            + (1.0 / (12.0 * (n_k + 0.5)))               -- + 1/(12x)
            - (1.0 / (360.0 * POW(n_k + 0.5, 3)))        -- - 1/(360 x^3)
          )
      ) AS sum_lngamma_nonzero,
      COUNT(*) AS nonzero_bins
    FROM MACRO_COUNTS MC
    GROUP BY MC.M
),
-- KNUTH_SCORES: Calculate the full Bayesian score F(M) for each candidate bin count M
-- Process: Compute F(M) = N ln(M) + ln Γ(M/2) - M ln Γ(1/2) - ln Γ((N+M)/2) + Σ ln Γ(n_k+1/2)
-- Input: TERMS (log-gamma sums), LOG_STATS (sample size N), STIRLING_CONST (gamma constants)
-- Output: Bayesian evidence F(M) for each candidate M (higher scores = better histogram fit)
KNUTH_SCORES AS (
    SELECT 
      T.M,
      /* N ln M */
      (S.N * LN(T.M))
      /* + lnGamma(M/2) (Stirling) */
      + (
          (
            (( (T.M/2.0) - 0.5) * LN(T.M/2.0))
            - (T.M/2.0)
            + 0.5 * C.LN_2PI
            + (1.0 / (12.0 * (T.M/2.0)))
            - (1.0 / (360.0 * POW(T.M/2.0, 3)))
          )
        )
      /* - M lnGamma(1/2) */
      - (T.M * C.LN_GAMMA_HALF_CONST)
      /* - lnGamma((N+M)/2) (Stirling) */
      - (
          (
            (( ((S.N + T.M)/2.0) - 0.5) * LN( (S.N + T.M)/2.0 ))
            - ((S.N + T.M)/2.0)
            + 0.5 * C.LN_2PI
            + (1.0 / (12.0 * ((S.N + T.M)/2.0)))
            - (1.0 / (360.0 * POW((S.N + T.M)/2.0, 3)))
          )
        )
      /* + Σ lnGamma(n_k+1/2) incluyendo bins vacíos: añadimos (M - nonzero) * lnGamma(1/2) */
      + (T.sum_lngamma_nonzero + (T.M - T.nonzero_bins) * C.LN_GAMMA_HALF_CONST)
      AS F_SCORE
    FROM TERMS T
    JOIN LOG_STATS S ON TRUE
    JOIN STIRLING_CONST C ON TRUE
),
-- BEST_M: Select the optimal bin count that maximizes the Bayesian score
-- Process: Find M* = argmax F(M) - the bin count with highest Bayesian evidence
-- Input: KNUTH_SCORES (F(M) scores for all candidates)
-- Output: Single optimal_bin_count M* that best balances model complexity vs fit
BEST_M AS (
    SELECT M AS optimal_bin_count
    FROM KNUTH_SCORES
    ORDER BY F_SCORE DESC
    LIMIT 1
),

/* ===== HISTOGRAM CONSTRUCTION WITH OPTIMAL BIN COUNT =====
   - Calculate bin edges in log space using M* bins, then transform back to USD
   - Count frequencies per bin using the same micro-to-macro bin mapping (u -> k) with optimal M*
*/
-- FINAL_COUNTS: Map micro-bins to the optimal number of bins and aggregate counts
-- Process: Use optimal M* to map micro-bins u to final bins k, aggregate order counts
-- Input: MICRO_COUNTS (micro-bin frequencies), BEST_M (optimal M*), PARAMS (U)
-- Output: Frequency count for each final histogram bin k using optimal bin count
FINAL_COUNTS AS (
    SELECT 
      CAST(FLOOR( (MC.u * B.optimal_bin_count) / P.U ) AS INT) AS k,
      SUM(MC.c) AS freq
    FROM MICRO_COUNTS MC
    JOIN BEST_M B ON TRUE
    JOIN PARAMS P ON TRUE
    GROUP BY CAST(FLOOR( (MC.u * B.optimal_bin_count) / P.U ) AS INT)
),
-- BINS_ALL: Generate a sequence of bin indices from 0 to M*-1
-- Process: Create complete sequence of bin indices for the optimal histogram
-- Input: BEST_M (optimal bin count M*)
-- Output: Sequential bin indices k = 0, 1, 2, ..., M*-1 for complete histogram coverage
BINS_ALL AS (
    -- Generate all k = 0..M*-1 (large ROWCOUNT and filter)
    SELECT 
      ROW_NUMBER() OVER (ORDER BY seq4()) - 1 AS k
    FROM TABLE(GENERATOR(ROWCOUNT=>100000))
    JOIN BEST_M B ON TRUE
    QUALIFY k < B.optimal_bin_count
),
-- LOG_EDGES: Calculate the log-space bin edges for the optimal bin count
-- Process: Divide log range [Y_MIN, Y_MAX] into M* equal-width intervals
-- Input: BINS_ALL (bin indices), BEST_M (optimal M*), LOG_STATS (log bounds)
-- Output: Left and right log-space boundaries (y_left, y_right) for each bin k
LOG_EDGES AS (
    SELECT 
      BA.k,
      S.Y_MIN + (S.Y_MAX - S.Y_MIN) * (BA.k      / B.optimal_bin_count) AS y_left,
      S.Y_MIN + (S.Y_MAX - S.Y_MIN) * ((BA.k+1.0)/ B.optimal_bin_count) AS y_right
    FROM BINS_ALL BA
    JOIN BEST_M B  ON TRUE
    JOIN LOG_STATS S ON TRUE
),
-- USD_BINS: Transform log-space bin edges back to USD values
-- Process: Convert log boundaries to USD using exp(), handle boundary conditions for first/last bins
-- Input: LOG_EDGES (log boundaries), BEST_M (for boundary detection), REJECTED_TABLE (for exact min/max)
-- Output: USD bin edges (edge_left_usd, edge_right_usd) and geometric center for each bin
USD_BINS AS (
    SELECT 
      L.k,
      CASE WHEN L.k = 0 THEN 
        (SELECT MIN(PAYMENT_AMOUNT_USD) FROM REJECTED_TABLE WHERE PAYMENT_AMOUNT_USD > 0)
      ELSE EXP(L.y_left) END AS edge_left_usd,
      CASE WHEN L.k = B.optimal_bin_count - 1 THEN 
        (SELECT MAX(PAYMENT_AMOUNT_USD) FROM REJECTED_TABLE)
      ELSE EXP(L.y_right) END AS edge_right_usd,
      EXP( (L.y_left + L.y_right)/2.0 ) AS bin_center_usd
    FROM LOG_EDGES L
    JOIN BEST_M B ON TRUE
),
-- HIST: Create the final logarithmic-scale histogram by combining bin definitions with frequencies
-- Process: LEFT JOIN bin definitions with frequency counts, fill missing bins with 0 frequency
-- Input: USD_BINS (bin definitions in USD), FINAL_COUNTS (bin frequencies)
-- Output: Complete histogram with bin boundaries in USD and frequencies (including zero-frequency bins)
HIST AS (
    SELECT 
      U.k,
      U.edge_left_usd,
      U.edge_right_usd,
      U.bin_center_usd,
      COALESCE(F.freq, 0) AS frequency
    FROM USD_BINS U
    LEFT JOIN FINAL_COUNTS F USING (k)
),
-- =============================================================================
-- STAGE 3: FIRST-LEVEL FILTER BASED ON LOGARITHMIC ANALYSIS  
-- =============================================================================
-- LOG_FIRST_FILTER: Sort bins by frequency, calculate cumulative metrics, mark top 90% bins
-- Process: Rank bins by frequency DESC, compute cumulative percentages, flag bins before 90% threshold
-- Input: HIST (complete logarithmic histogram with USD boundaries and frequencies)
-- Output: Ranked bins with cumulative metrics and in_range_of_interest flag (1 for top 90%, 0 otherwise)
LOG_FIRST_FILTER AS (
-- === First-stage output with bin statistics ===
SELECT 
  ROW_NUMBER() OVER (ORDER BY frequency DESC) AS bin_order, -- order by frequency (highest first)
  (k + 1) AS bin_number,        -- 1-indexed bin number
  edge_left_usd AS bin_start,   -- lower bound in USD
  edge_right_usd AS bin_end,    -- upper bound in USD
  bin_center_usd AS geometric_bin_center, -- geometric center in USD
  (edge_left_usd + edge_right_usd)/2.0 AS bin_center, -- arithmetic mean of the boundaries
  (edge_right_usd - edge_left_usd) AS bin_width, -- width of the bin
  --CUMULATIVE FREQUENCY
  SUM(frequency) OVER (ORDER BY frequency DESC) AS cumulative_frequency,
  -- CUMULATIVE FREQUENCY PERCENTAGE
  SUM(frequency) OVER (ORDER BY frequency DESC) / (SELECT COUNT(*) FROM REJECTED_TABLE) AS cumulative_frequency_percentage,
  -- RANGE OF INTEREST INDICATOR (BINS BEFORE 90% CUMULATIVE FREQUENCY)
  CASE 
    WHEN SUM(frequency) OVER (ORDER BY frequency DESC) / (SELECT COUNT(*) FROM REJECTED_TABLE) < 0.9 THEN 1
    ELSE 0
  END AS in_range_of_interest,
  frequency
FROM HIST
ORDER BY frequency DESC
),
-- =============================================================================
-- STAGE 4: FILTER REJECTED ORDERS BASED ON LOG-SCALE ANALYSIS
-- =============================================================================
-- FILTERED_REJECTED_TABLE: Filter original rejected orders using log-scale bin analysis results
-- Process: Keep only orders falling within bins marked as in_range_of_interest = 1 (top 90% by frequency)
-- Input: REJECTED_TABLE (all rejected orders), LOG_FIRST_FILTER (bins with range flags)
-- Output: Filtered subset of rejected orders (~90% of data) excluding outliers and rare amount ranges
FILTERED_REJECTED_TABLE AS (
-- Get all data from REJECTED_TABLE but only for orders that fall within bins marked as in_range_of_interest = 1
-- This effectively filters out outliers and focuses on the most significant 90% of orders by frequency
SELECT 
    REJECTED_TABLE.*
FROM REJECTED_TABLE
JOIN LOG_FIRST_FILTER ON REJECTED_TABLE.PAYMENT_AMOUNT_USD BETWEEN LOG_FIRST_FILTER.bin_start AND LOG_FIRST_FILTER.bin_end
WHERE LOG_FIRST_FILTER.in_range_of_interest = 1
),

-- =============================================================================
-- STAGE 5: SECOND-LEVEL KNUTH ANALYSIS ON LINEAR SCALE
-- =============================================================================
-- Repeat the Knuth method on filtered data using linear USD scale (no log transformation)
-- Focus on top 90% most frequent bins in this refined analysis
-- Result: approximately 81% of original data (90% of the initial 90%)

/* ========= Knuth Method on Linear USD Scale (with pre-aggregation) ========= */
-- This section repeats the Knuth method on raw USD values rather than log-transformed values
-- Same computational approach but working directly with dollar amounts on the filtered dataset

-- PARAMS_2: Define parameters for the linear-scale Knuth algorithm (second stage)
-- Process: Set constants for micro-bin count, bin count range, and geometric progression step
-- Input: Algorithm constants (same values as stage 1 for consistency)
-- Output: Configuration parameters for second-stage analysis (U=20000, M range 64-4096, step 1.03)
PARAMS_2 AS (
    SELECT 
        20000::INT AS U,               -- micro-bins in linear USD space
        64::INT    AS M_MIN,           -- minimum bin count to evaluate
        4096::INT  AS M_MAX,           -- maximum bin count to evaluate
        1.03::FLOAT AS M_STEP          -- geometric step size for M grid exploration
),
-- STATS_2: Calculate basic statistics for the filtered payment amounts (linear scale)
-- Process: Compute count, min, and max of USD payment amounts from FILTERED_REJECTED_TABLE
-- Input: FILTERED_REJECTED_TABLE (90% filtered rejected orders)
-- Output: N (sample size), Y_MIN (minimum USD), Y_MAX (maximum USD) for linear histogram bounds
STATS_2 AS (
    SELECT 
        COUNT(*)::INT AS N,
        MIN(PAYMENT_AMOUNT_USD) AS Y_MIN,
        MAX(PAYMENT_AMOUNT_USD) AS Y_MAX
    FROM FILTERED_REJECTED_TABLE
    WHERE PAYMENT_AMOUNT_USD > 0
),
-- MGRID_2: Generate a geometric grid of potential bin counts (M values) for linear analysis
-- Process: Create sequence from M_MIN to M_MAX using geometric progression (same as stage 1)
-- Input: PARAMS_2 (algorithm parameters)
-- Output: Distinct integer M values from 64 to 4096 for second-stage Bayesian evaluation
MGRID_2 AS (
    SELECT DISTINCT CAST(ROUND(m_val) AS INT) AS M
    FROM (
        SELECT P.M_MIN * POWER(P.M_STEP, SEQ4()) AS m_val
        FROM PARAMS_2 P, TABLE(GENERATOR(ROWCOUNT=>300))
    )
    WHERE m_val BETWEEN (SELECT M_MIN FROM PARAMS_2) AND (SELECT M_MAX FROM PARAMS_2)
),
-- MICRO_BINS_2: Pre-aggregation step 1 - Assign each filtered payment amount to U micro-bins in linear space
-- Process: Map each USD amount to micro-bin index u using uniform division (no log transformation)
-- Input: FILTERED_REJECTED_TABLE (filtered payments), STATS_2 (USD bounds), PARAMS_2 (U=20000)
-- Output: Each filtered order mapped to linear micro-bin index u ∈ [0, U-1]
MICRO_BINS_2 AS (
    SELECT 
        LEAST(GREATEST(
            -- Use CEIL to assign boundary values to the higher bin (consistent with log-scale approach)
            CAST(CEIL( (T.PAYMENT_AMOUNT_USD - S.Y_MIN) / ((S.Y_MAX - S.Y_MIN) / P.U) ) - 1 AS INT)
        , 0), P.U-1) AS u
    FROM FILTERED_REJECTED_TABLE T
    JOIN STATS_2 S ON TRUE
    JOIN PARAMS_2 P ON TRUE
    WHERE T.PAYMENT_AMOUNT_USD > 0
),
-- MICRO_COUNTS_2: Count observations in each linear-scale micro-bin
-- Process: Aggregate MICRO_BINS_2 to count filtered orders in each linear micro-bin u
-- Input: MICRO_BINS_2 (filtered order-to-micro-bin mappings)
-- Output: Frequency count c for each linear micro-bin u (pre-aggregated filtered data)
MICRO_COUNTS_2 AS (
    SELECT u, COUNT(*)::INT AS c
    FROM MICRO_BINS_2
    GROUP BY u
),
-- MACRO_COUNTS_2: For each candidate M, map linear micro-bins to macro-bins and aggregate counts
-- Process: For each M candidate, map micro-bin u to macro-bin k, sum filtered order counts
-- Input: MICRO_COUNTS_2 (linear micro-bin frequencies), MGRID_2 (M candidates), PARAMS_2 (U)
-- Output: For each (M,k) pair, aggregated count n_k of filtered orders in linear macro-bin k
MACRO_COUNTS_2 AS (
    -- Formula: k = floor(u * M / U) to map micro-bin u to macro-bin k for a given M
    SELECT 
        M.M,
        CAST(FLOOR( (MC.u * M.M) / P.U ) AS INT) AS k,
        SUM(MC.c)::INT AS n_k
    FROM MICRO_COUNTS_2 MC
    CROSS JOIN MGRID_2 M
    JOIN PARAMS_2 P ON TRUE
    GROUP BY M.M, CAST(FLOOR( (MC.u * M.M) / P.U ) AS INT)
),
-- TERMS_2: Calculate the sum of ln(Gamma(n_k+1/2)) terms for each candidate M in linear analysis
-- Process: For each M, sum Stirling approximation of ln(Γ(n_k + 1/2)) across linear macro-bins
-- Input: MACRO_COUNTS_2 (linear bin frequencies), STIRLING_CONST (gamma constants)
-- Output: For each M, sum of log-gamma terms needed for second-stage Bayesian score F(M)
TERMS_2 AS (
    SELECT 
      MC.M,
      SUM(
          /* lnGamma(n_k + 1/2) via Stirling, x = n_k + 0.5 */
          (
            (( (n_k + 0.5) - 0.5) * LN(n_k + 0.5)) -- (x-0.5) ln x
            - (n_k + 0.5)                          -- -x
            + 0.5 * (SELECT LN_2PI FROM STIRLING_CONST)  -- + 0.5 ln(2π)
            + (1.0 / (12.0 * (n_k + 0.5)))               -- + 1/(12x)
            - (1.0 / (360.0 * POW(n_k + 0.5, 3)))        -- - 1/(360 x^3)
          )
      ) AS sum_lngamma_nonzero,
      COUNT(*) AS nonzero_bins
    FROM MACRO_COUNTS_2 MC
    GROUP BY MC.M
),
-- KNUTH_SCORES_2: Calculate the full Bayesian score F(M) for each candidate M in linear analysis
-- Process: Compute F(M) using same formula as stage 1 but with filtered linear data
-- Input: TERMS_2 (linear log-gamma sums), STATS_2 (filtered sample size), STIRLING_CONST
-- Output: Bayesian evidence F(M) for each candidate M using filtered linear data
KNUTH_SCORES_2 AS (
    SELECT 
      T.M,
      /* N ln M */
      (S.N * LN(T.M))
      /* + lnGamma(M/2) (Stirling) */
      + (
          (
            (( (T.M/2.0) - 0.5) * LN(T.M/2.0))
            - (T.M/2.0)
            + 0.5 * C.LN_2PI
            + (1.0 / (12.0 * (T.M/2.0)))
            - (1.0 / (360.0 * POW(T.M/2.0, 3)))
          )
        )
      /* - M lnGamma(1/2) */
      - (T.M * C.LN_GAMMA_HALF_CONST)
      /* - lnGamma((N+M)/2) (Stirling) */
      - (
          (
            (( ((S.N + T.M)/2.0) - 0.5) * LN( (S.N + T.M)/2.0 ))
            - ((S.N + T.M)/2.0)
            + 0.5 * C.LN_2PI
            + (1.0 / (12.0 * ((S.N + T.M)/2.0)))
            - (1.0 / (360.0 * POW((S.N + T.M)/2.0, 3)))
          )
        )
      /* + Σ lnGamma(n_k+1/2) incluyendo bins vacíos: añadimos (M - nonzero) * lnGamma(1/2) */
      + (T.sum_lngamma_nonzero + (T.M - T.nonzero_bins) * C.LN_GAMMA_HALF_CONST)
      AS F_SCORE
    FROM TERMS_2 T
    JOIN STATS_2 S ON TRUE
    JOIN STIRLING_CONST C ON TRUE
),
-- BEST_M_2: Select the optimal bin count for linear-scale analysis
-- Process: Find M* = argmax F(M) for the filtered linear data
-- Input: KNUTH_SCORES_2 (F(M) scores for linear analysis)
-- Output: Single optimal_bin_count M* for the second-stage linear histogram
BEST_M_2 AS (
    SELECT M AS optimal_bin_count
    FROM KNUTH_SCORES_2
    ORDER BY F_SCORE DESC
    LIMIT 1
),
-- FINAL_COUNTS_2: Map linear micro-bins to the optimal number of bins and aggregate counts
-- Process: Use optimal M* from linear analysis to create final bin frequencies
-- Input: MICRO_COUNTS_2 (linear micro-bin frequencies), BEST_M_2 (optimal linear M*), PARAMS_2
-- Output: Frequency count for each final linear histogram bin k using optimal linear bin count
FINAL_COUNTS_2 AS (
    SELECT 
      CAST(FLOOR( (MC.u * B.optimal_bin_count) / P.U ) AS INT) AS k,
      SUM(MC.c) AS freq
    FROM MICRO_COUNTS_2 MC
    JOIN BEST_M_2 B ON TRUE
    JOIN PARAMS_2 P ON TRUE
    GROUP BY CAST(FLOOR( (MC.u * B.optimal_bin_count) / P.U ) AS INT)
),
-- BINS_ALL_2: Generate a sequence of bin indices from 0 to M*-1 for linear bins
-- Process: Create complete sequence of bin indices for the optimal linear histogram
-- Input: BEST_M_2 (optimal linear bin count M*)
-- Output: Sequential bin indices k = 0, 1, 2, ..., M*-1 for linear histogram coverage
BINS_ALL_2 AS (
    -- Generate all k = 0..M*-1
    SELECT 
      ROW_NUMBER() OVER (ORDER BY seq4()) - 1 AS k
    FROM TABLE(GENERATOR(ROWCOUNT=>100000))
    JOIN BEST_M_2 B ON TRUE
    QUALIFY k < B.optimal_bin_count
),
-- LINEAR_EDGES: Calculate the linear-space bin edges for the optimal bin count
-- Process: Divide USD range [Y_MIN, Y_MAX] into M* equal-width intervals (no log transformation)
-- Input: BINS_ALL_2 (linear bin indices), BEST_M_2 (optimal linear M*), STATS_2 (USD bounds)
-- Output: Left and right USD boundaries (y_left, y_right) for each linear bin k
LINEAR_EDGES AS (
    SELECT 
      BA.k,
      S.Y_MIN + (S.Y_MAX - S.Y_MIN) * (BA.k      / B.optimal_bin_count) AS y_left,
      S.Y_MIN + (S.Y_MAX - S.Y_MIN) * ((BA.k+1.0)/ B.optimal_bin_count) AS y_right
    FROM BINS_ALL_2 BA
    JOIN BEST_M_2 B ON TRUE
    JOIN STATS_2 S ON TRUE
),
-- USD_BINS_2: Define the bin edges in USD values for linear-scale analysis
-- Process: Use linear boundaries directly (no exp transformation), handle boundary conditions
-- Input: LINEAR_EDGES (linear USD boundaries), BEST_M_2 (boundary detection), FILTERED_REJECTED_TABLE (min/max)
-- Output: USD bin edges and centers for linear analysis (edge_left_usd, edge_right_usd, bin_center_usd)
USD_BINS_2 AS (
    SELECT 
      L.k,
      CASE WHEN L.k = 0 THEN 
        (SELECT MIN(PAYMENT_AMOUNT_USD) FROM FILTERED_REJECTED_TABLE WHERE PAYMENT_AMOUNT_USD > 0)
      ELSE L.y_left END AS edge_left_usd,
      CASE WHEN L.k = B.optimal_bin_count - 1 THEN 
        (SELECT MAX(PAYMENT_AMOUNT_USD) FROM FILTERED_REJECTED_TABLE)
      ELSE L.y_right END AS edge_right_usd,
      (L.y_left + L.y_right)/2.0 AS bin_center_usd
    FROM LINEAR_EDGES L
    JOIN BEST_M_2 B ON TRUE
),
-- HIST_2: Create the final linear-scale histogram by combining bin definitions with frequencies
-- Process: LEFT JOIN linear bin definitions with frequency counts, fill missing bins with 0
-- Input: USD_BINS_2 (linear bin definitions), FINAL_COUNTS_2 (linear bin frequencies)
-- Output: Complete linear histogram with USD boundaries and frequencies (including zero-frequency bins)
HIST_2 AS (
    SELECT
      U.k,
      U.edge_left_usd,
      U.edge_right_usd,
      U.bin_center_usd,
      COALESCE(F.freq, 0) AS freq
    FROM USD_BINS_2 U
    LEFT JOIN FINAL_COUNTS_2 F USING (k)
),
-- =============================================================================
-- STAGE 6: SECOND-LEVEL FILTER BASED ON LINEAR ANALYSIS
-- =============================================================================
-- LINEAR_SECOND_FILTER: Create second-level filter using linear analysis results
-- Process: Rank linear bins by frequency, compute cumulative metrics, flag top 90% of filtered data
-- Input: HIST_2 (complete linear histogram with USD boundaries and frequencies)
-- Output: Ranked linear bins with cumulative metrics and in_range_of_interest flag for final filtering
LINEAR_SECOND_FILTER AS (
SELECT 
    k,
    edge_left_usd,
    edge_right_usd,
    bin_center_usd,
    freq,
    -- Cumulative frequency
    SUM(freq) OVER (ORDER BY freq DESC) AS cumulative_freq,
    -- Cumulative frequency percentage
    SUM(freq) OVER (ORDER BY freq DESC) / SUM(freq) OVER () AS cumulative_freq_pct,
    -- Indicator for bins before 90% threshold
    CASE 
        WHEN SUM(freq) OVER (ORDER BY freq DESC) / SUM(freq) OVER () < 0.9 THEN 1
        ELSE 0
    END AS in_range_of_interest
FROM HIST_2 
ORDER BY freq DESC
),
-- =============================================================================
-- STAGE 7: EXTRACT FINAL ORDERS OF INTEREST
-- =============================================================================
-- FINAL_REJECTED_TABLE: Extract orders passing both logarithmic and linear filtering stages
-- Process: Further filter FILTERED_REJECTED_TABLE using linear analysis results (double filtering)
-- Input: FILTERED_REJECTED_TABLE (90% filtered), LINEAR_SECOND_FILTER (linear bins with flags)
-- Output: Final subset of orders (~81% of original: 90% × 90%) representing core payment amount ranges
FINAL_REJECTED_TABLE AS (
  SELECT 
    FILTERED_REJECTED_TABLE.*
FROM FILTERED_REJECTED_TABLE
JOIN LINEAR_SECOND_FILTER ON FILTERED_REJECTED_TABLE.PAYMENT_AMOUNT_USD BETWEEN LINEAR_SECOND_FILTER.edge_left_usd AND LINEAR_SECOND_FILTER.edge_right_usd
WHERE LINEAR_SECOND_FILTER.in_range_of_interest = 1
),

-- =============================================================================
-- STAGE 8: BUILD FINAL 1-USD BIN HISTOGRAM
-- =============================================================================
-- Build the final histogram with 1-USD bin width from floor(min) to ceil(max) of FINAL_REJECTED_TABLE
-- This creates precise 1-dollar bins covering the core interest range

-- PARAMS_3: Calculate bounds for 1-USD bin histogram
-- Process: Determine floor(min) and ceil(max) payment amounts from final filtered orders
-- Input: FINAL_REJECTED_TABLE (core ~81% of orders)
-- Output: bin_min (floor of minimum) and bin_max (ceil of maximum) for 1-USD bin boundaries
PARAMS_3 AS (
    SELECT 
        FLOOR(MIN(PAYMENT_AMOUNT_USD)) AS bin_min,
        CEIL(MAX(PAYMENT_AMOUNT_USD)) AS bin_max
    FROM FINAL_REJECTED_TABLE
),
-- BINS_3: Generate bins from min to max with width of 1 USD
-- Process: Create sequence of 1-USD bins covering [bin_min, bin_max] range
-- Input: PARAMS_3 (bin boundaries)
-- Output: Sequential 1-USD bins with k index, left/right edges, and center points
BINS_3 AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY seq4()) - 1 AS k,
        P.bin_min + (ROW_NUMBER() OVER (ORDER BY seq4()) - 1) AS edge_left_usd,
        P.bin_min + (ROW_NUMBER() OVER (ORDER BY seq4())) AS edge_right_usd,
        P.bin_min + (ROW_NUMBER() OVER (ORDER BY seq4()) - 0.5) AS bin_center_usd
    FROM TABLE(GENERATOR(ROWCOUNT=>100000)) t
    JOIN PARAMS_3 P
    QUALIFY edge_left_usd < P.bin_max
),
-- FINAL_COUNTS_3: Count frequency of orders in each 1-USD bin
-- Process: Count how many final filtered orders fall within each 1-USD bin
-- Input: FINAL_REJECTED_TABLE (core orders), BINS_3 (1-USD bin definitions)
-- Output: Frequency count for each 1-USD bin k containing final filtered orders
FINAL_COUNTS_3 AS (
    SELECT 
        B.k,
        COUNT(*) AS freq
    FROM FINAL_REJECTED_TABLE F
    JOIN BINS_3 B ON F.PAYMENT_AMOUNT_USD >= B.edge_left_usd AND F.PAYMENT_AMOUNT_USD < B.edge_right_usd
    GROUP BY B.k
),
-- HIST_3: Create the final 1-USD histogram by combining bin definitions with frequencies
-- Process: LEFT JOIN 1-USD bin definitions with order counts, fill empty bins with 0 frequency
-- Input: BINS_3 (1-USD bin definitions), FINAL_COUNTS_3 (bin frequencies)
-- Output: Complete 1-USD histogram covering the core interest range with all bins (including empty ones)
HIST_3 AS (
    SELECT
        B.k,
        B.edge_left_usd,
        B.edge_right_usd,
        B.bin_center_usd,
        COALESCE(F.freq, 0) AS freq
    FROM BINS_3 B
    LEFT JOIN FINAL_COUNTS_3 F USING (k)
),

-- Create intermediate histogram with cumulative frequency
HIST_WITH_CUMULATIVE AS (
    SELECT 
        (k + 1) AS bin_number,
        edge_left_usd AS bin_start,
        edge_right_usd AS bin_end,
        bin_center_usd AS bin_center,
        (edge_right_usd - edge_left_usd) AS bin_width,
        freq AS frequency,
        CASE WHEN freq > 0 THEN 1 ELSE 0 END AS has_frequency,
        CASE WHEN freq > 0 THEN 0 ELSE 1 END AS not_has_frequency,
        SUM(CASE WHEN freq > 0 THEN 1 ELSE 0 END) OVER (ORDER BY k) AS cumulative_has_frequency
    FROM HIST_3
),

-- First stage: Mark the beginning of sequences of 1s
SEQUENCE_START AS (
    SELECT
        bin_number,
        bin_start,
        bin_end,
        frequency,
        has_frequency,
        CASE
            WHEN has_frequency = 1 AND COALESCE(LAG(has_frequency) OVER (ORDER BY bin_number), 0) = 0 THEN 1
            ELSE 0
        END AS start_flag
    FROM HIST_WITH_CUMULATIVE
),

-- Second stage: Group by continuous sequences
GROUPED_SEQUENCES AS (
    SELECT
        bin_number,
        bin_start,
        bin_end,
        frequency,
        has_frequency,
        SUM(start_flag) OVER (ORDER BY bin_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sequence_group
    FROM SEQUENCE_START
),

-- Third stage: Calculate metrics for each continuous sequence
CONTINUOUS_RUNS AS (
    SELECT
        sequence_group,
        MIN(bin_number) AS first_bin,
        MAX(bin_number) AS last_bin,
        MIN(bin_start) AS start_amount,
        MAX(bin_end) AS end_amount,
        COUNT(*) AS run_length,
        SUM(frequency) AS total_frequency
    FROM GROUPED_SEQUENCES
    WHERE has_frequency = 1
    GROUP BY sequence_group
),

-- Final result (modified to include only the longest sequence)
FINAL_RESULT AS (
    SELECT
        *,
        end_amount - start_amount AS interval_width,
        'LARGE_INTEREST_INTERVAL' AS interval_type
    FROM CONTINUOUS_RUNS
    QUALIFY run_length = MAX(run_length) OVER ()
),

-- Top 3 bins by frequency
TOP_BINS AS (
    SELECT
        edge_left_usd AS start_amount,
        edge_right_usd AS end_amount,
        freq AS bin_frequency,
        CASE 
            WHEN ROW_NUMBER() OVER (ORDER BY freq DESC) = 1 THEN 'SHORT_INTEREST_INTERVAL_1'
            WHEN ROW_NUMBER() OVER (ORDER BY freq DESC) = 2 THEN 'SHORT_INTEREST_INTERVAL_2'
            WHEN ROW_NUMBER() OVER (ORDER BY freq DESC) = 3 THEN 'SHORT_INTEREST_INTERVAL_3'
        END AS interval_type
    FROM HIST_3
    QUALIFY ROW_NUMBER() OVER (ORDER BY freq DESC) <= 3
),

-- Extract minimum and maximum values for PRE and POST intervals
GRANDE_INTERVAL_BOUNDS AS (
    SELECT 
        MIN(start_amount) AS grande_start,
        MAX(end_amount) AS grande_end,
        (SELECT MAX(PAYMENT_AMOUNT_USD) + 1 FROM REJECTED_TABLE) AS max_amount
    FROM FINAL_RESULT
),

-- Create PRE and POST intervals
PRE_POST_INTERVALS AS (
    SELECT
        0 AS start_amount,
        grande_start AS end_amount,
        'PRE_LARGE_INTERVAL' AS interval_type,
        (SELECT COUNT(*) FROM REJECTED_TABLE WHERE PAYMENT_AMOUNT_USD BETWEEN 0 AND grande_start) AS frequency
    FROM GRANDE_INTERVAL_BOUNDS
    
    UNION ALL
    
    SELECT
        grande_end AS start_amount,
        max_amount AS end_amount,
        'POST_LARGE_INTERVAL' AS interval_type,
        (SELECT COUNT(*) FROM REJECTED_TABLE WHERE PAYMENT_AMOUNT_USD BETWEEN grande_end AND max_amount) AS frequency
    FROM GRANDE_INTERVAL_BOUNDS
),

-- Filter HIST_3 for only the bins within the main interval of interest
LARGE_INTEREST_INTERVAL_TABLE AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY seq) - 1 AS k,
        -- First bin starts exactly at start_amount
        CASE WHEN seq = 1 THEN start_amount ELSE start_amount + seq - 1 END AS bin_start,
        -- Last bin ends exactly at end_amount
        CASE WHEN seq = CEIL(end_amount - start_amount) THEN end_amount ELSE start_amount + seq END AS bin_end,
        (SELECT COUNT(*) 
         FROM REJECTED_TABLE 
         WHERE PAYMENT_AMOUNT_USD >= 
            (CASE WHEN seq = 1 THEN start_amount ELSE start_amount + seq - 1 END)
         AND PAYMENT_AMOUNT_USD < 
            (CASE WHEN seq = CEIL(end_amount - start_amount) THEN end_amount ELSE start_amount + seq END)
        ) AS frequency
    FROM (
        SELECT F.start_amount, F.end_amount, seq4() AS seq
        FROM FINAL_RESULT F
        CROSS JOIN TABLE(GENERATOR(ROWCOUNT=>100000))
        WHERE F.interval_type = 'LARGE_INTEREST_INTERVAL'
        QUALIFY seq <= CEIL(F.end_amount - F.start_amount)
    )
),
-- Calculate total of rejected orders for the percentage
TOTAL_REJECTED AS (
    SELECT COUNT(*) AS total_rejected_orders FROM REJECTED_TABLE
),
TOTAL_REJECTED_LARGE_INTEREST_INTERVAL AS (
    SELECT COUNT(*) AS total_rejected_orders FROM REJECTED_TABLE
    WHERE PAYMENT_AMOUNT_USD BETWEEN (SELECT start_amount FROM FINAL_RESULT WHERE interval_type = 'LARGE_INTEREST_INTERVAL') AND (SELECT end_amount FROM FINAL_RESULT WHERE interval_type = 'LARGE_INTEREST_INTERVAL')
),
-- Calculate percentage of rejected orders per bin
PERCENTAGE_REJECTED AS (
    SELECT
        k,
        bin_start,
        bin_end,
        frequency,
        --cumulative frequency excluding k=0
        SUM(CASE WHEN k > 0 THEN frequency ELSE 0 END) OVER (ORDER BY k) AS cumulative_frequency,
        --cumulative percentage frequency rounded to two decimal places
        CAST((SUM(CASE WHEN k > 0 THEN frequency ELSE 0 END) OVER (ORDER BY k) / 
             (SELECT SUM(frequency) FROM LARGE_INTEREST_INTERVAL_TABLE WHERE k > 0)) * 100 AS DECIMAL(10,2)) AS cumulative_percentage_relative_to_total_orders_of_interest,
        --cumulative percentage frequency rounded to two decimal places
        CAST((SUM(CASE WHEN k > 0 THEN frequency ELSE 0 END) OVER (ORDER BY k) / (SELECT total_rejected_orders FROM TOTAL_REJECTED)) * 100 AS DECIMAL(10,2)) AS cumulative_percentage_relative_to_total_rejected_orders,
        CAST((SUM(CASE WHEN k > 0 THEN frequency ELSE 0 END) OVER (ORDER BY k) / (k*(SELECT total_rejected_orders FROM TOTAL_REJECTED))) * 100 AS DECIMAL(10,2)) AS cumulative_orders_by_considered_interval_width
    FROM LARGE_INTEREST_INTERVAL_TABLE
),
PERCENTAGE_REJECTED_2 AS (
SELECT * FROM PERCENTAGE_REJECTED WHERE k > 0 ORDER BY k),

-- Summary table with three interval types: LARGE, MEDIUM, SMALL
SUMMARY_TABLE AS (
    -- LARGE interval: from first to last bin
    SELECT
        (SELECT MIN(bin_start) FROM PERCENTAGE_REJECTED_2) AS init,
        (SELECT MAX(bin_end) FROM PERCENTAGE_REJECTED_2) AS end,
        (SELECT SUM(frequency) FROM PERCENTAGE_REJECTED_2) AS frequency,
        'LARGE' AS interval_type
    
    UNION ALL
    
    -- MEDIUM interval: from first bin to bin where cumulative percentage crosses 80%
    SELECT
        (SELECT MIN(bin_start) FROM PERCENTAGE_REJECTED_2) AS init,
        (SELECT bin_end FROM PERCENTAGE_REJECTED_2 
         WHERE cumulative_percentage_relative_to_total_rejected_orders >= 80
         ORDER BY k
         LIMIT 1) AS end,
        (SELECT SUM(frequency) FROM PERCENTAGE_REJECTED_2 
         WHERE bin_end <= (SELECT bin_end FROM PERCENTAGE_REJECTED_2 
                          WHERE cumulative_percentage_relative_to_total_rejected_orders >= 80
                          ORDER BY k
                          LIMIT 1)) AS frequency,
        'MEDIUM' AS interval_type
        
    UNION ALL
    
    -- SMALL interval: bin with highest frequency
    SELECT
        P.bin_start AS init,
        P.bin_end AS end,
        P.frequency AS frequency,
        'SMALL' AS interval_type
    FROM (
        SELECT bin_start, bin_end, frequency
        FROM PERCENTAGE_REJECTED_2
        ORDER BY frequency DESC
        LIMIT 1
    ) P
)

-- FINAL OUTPUT: Return the summary table with interval_type as the first column
-- This table provides three strategic interval types (LARGE, MEDIUM, SMALL) with their metrics
SELECT 
    interval_type,                  -- Primary classification: LARGE (full range), MEDIUM (up to 80% cumulative), SMALL (highest frequency bin)
    init as USD_BIN_INIT,           -- Starting USD amount of the interval
    end as USD_BIN_END,             -- Ending USD amount of the interval 
    end - init AS bin_width,        -- Width of the interval in USD
    frequency,                      -- Number of rejected orders in this interval
    -- Frequency normalized by interval width and total rejected orders (percentage)
    ROUND((frequency / ((SELECT COUNT(*) FROM REJECTED_TABLE)*(end - init))) * 100, 2) AS percentage_of_rejected_orders_divided_by_width,
    -- Percentage of this interval's frequency relative to total rejected orders
    ROUND((frequency / (SELECT COUNT(*) FROM REJECTED_TABLE)) * 100, 2) AS percentage_of_rejected_orders
FROM SUMMARY_TABLE
ORDER BY CASE interval_type 
    WHEN 'LARGE' THEN 1
    WHEN 'MEDIUM' THEN 2
    WHEN 'SMALL' THEN 3
END;
/*
 * ADDITIONAL MATHEMATICAL NOTES: KNUTH METHOD (BAYESIAN)
 * 
 * 1. Foundation of the Knuth Method for Optimal Bins:
 *    The Knuth method uses Bayesian evidence to find the optimal
 *    number of bins M* that maximizes the marginal likelihood function.
 *    This avoids the over-binning problems of the Freedman-Diaconis method
 *    with large-scale financial data and asymmetric distributions.
 *    
 *    F(M) = N ln(M) + ln Γ(M/2) - M ln Γ(1/2) - ln Γ((N+M)/2) + Σₖ ln Γ(nₖ + 1/2)
 *    
 *    Where:
 *    - N = total number of observations
 *    - M = candidate bin count
 *    - nₖ = number of observations in bin k
 *    - Γ = gamma function
 *
 * 2. Logarithmic Transformation for Financial Data:
 *    We apply y = ln(PAYMENT_AMOUNT_USD) to:
 *    - **Stabilize variance**: Reduces the impact of right-tail outliers
 *    - **Uniform width**: Bins in log scale have constant width
 *    - **Improve convergence**: Bayesian optimization is more stable
 *    - **Multiplicative interpretation**: Ranges represent factors, not differences
 *
 * 3. Pre-aggregation in Micro-bins (Computational Efficiency):
 *    For scalability in Snowflake with millions of records:
 *    - **Step 1**: Divide log range into U=20,000 uniform micro-bins
 *    - **Step 2**: Count observations per micro-bin (single data pass)
 *    - **Step 3**: Re-aggregate micro-bins to each M candidate: k = ⌊u × M / U⌋
 *    - **Advantage**: We evaluate M=64 to M=4096 without touching original data
 *
 * 4. Geometric Grid of M Candidates:
 *    M = round(M_MIN × M_STEP^t) where M_STEP = 1.03
 *    - **Range**: M ∈ [64, 4096] (balancing granularity and efficiency)
 *    - **Geometric progression**: Explores the space efficiently
 *    - **No duplicates**: DISTINCT guarantees unique evaluation per M
 *
 * 5. Interpretation of Bayesian Score F(M):
 *    - **Maximization**: M* = argmax F(M) optimally balances bias-variance
 *    - **Complexity component**: N ln(M) penalizes excess bins
 *    - **Fit component**: Σₖ ln Γ(nₖ + 1/2) rewards well-populated bins
 *    - **Bayesian regularization**: Gamma terms implement informative prior
 *
 * 6. Calculation of Limits in Original Scale (USD):
 *    For bin i in logarithmic scale (i = 0, 1, ..., M*-1):
 *    - **Log-lower limit**: y_min + (y_max - y_min) × i/M*
 *    - **Log-upper limit**: y_min + (y_max - y_min) × (i+1)/M*
 *    - **USD-lower limit**: exp(log-lower limit)
 *    - **USD-upper limit**: exp(log-upper limit)
 *    - **Geometric center**: √(USD-lower × USD-upper)
 *
 * 7. Advantages of the Knuth Method for Financial Analysis:
 *    - **Statistical robustness**: Does not depend on IQR or standard deviation
 *    - **Automatic adaptation**: Adjusts to the actual shape of the distribution
 *    - **Scalability**: Pre-aggregation allows efficient handling of Big Data
 *    - **Multiplicative interpretation**: Bins represent proportional amount ranges
 *    - **Global optimization**: Finds optimal balance between resolution and noise
 */