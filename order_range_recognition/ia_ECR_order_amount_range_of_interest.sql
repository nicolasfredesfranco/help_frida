-- ============================================================================= 
-- Query: ia_ECR_order_amount_range_of_interest.sql
-- =============================================================================
-- PURPOSE: Analyzes the distribution of ECR (Effective Cost Rate) across approved order value amounts.
-- OBJECTIVE: Identify payment amount ranges with highest ECR concentration for strategic cost optimization.
-- METHODOLOGY: Uses advanced statistical analysis with Knuth (Bayesian) optimal binning method.
--
-- BUSINESS PROBLEM SOLVED:
-- - Where do we have the highest processing costs (ECR) concentration by payment amount?
-- - Which payment amount intervals should we prioritize for cost reduction efforts?
-- - How can we strategically target the most cost-impactful transaction ranges?
--
-- ANALYTICAL APPROACH:
-- This query systematically identifies payment amount ranges containing 90% of total ECR through:
--   1. LOGARITHMIC SCALE ANALYSIS: Handles wide range of payment amounts (outliers)
--   2. LINEAR SCALE REFINEMENT: Fine-tunes analysis on filtered high-ECR subset
--   3. CONTINUOUS INTERVAL DETECTION: Finds longest sequence with highest ECR concentration
--
-- STATISTICAL METHOD: Knuth (Bayesian) method for optimal histogram binning
-- - Automatically determines optimal number of bins M* that maximizes Bayesian evidence
-- - Avoids over-binning issues of traditional methods (Freedman-Diaconis)
-- - Handles asymmetric ECR distributions robustly
--
-- COMPUTATIONAL OPTIMIZATION: Two-stage pre-aggregation for Big Data scalability
-- - Stage 1: 20,000 micro-bins for efficient data aggregation
-- - Stage 2: Dynamic re-binning using geometric grid search (M=64 to M=4096)
-- - Result: Evaluates thousands of bin configurations without repeated data scans

-- =============================================================================
-- STAGE 1: DATA EXTRACTION AND PREPARATION - USING BASE QUERY
-- =============================================================================
-- PURPOSE: Import standardized order data with ECR calculations from base_query.sql
-- INPUT SOURCE: ORDER_TABLE from base_query.sql (6 months of approved orders)
-- OUTPUT: Foundation dataset for ECR distribution analysis
-- WHY THIS APPROACH: Reuses established data pipeline and ensures consistency

WITH BASE_DATA AS (
    SELECT
        o.ORDER_ID,
        o.ORDER_DATE,
        o.ORDER_TIME,
        o.COMMERCE_ID,
        DATE_TRUNC('month', o.ORDER_DATE) AS DATE_MONTH,
        o.ORDER_CITY_NAME,
        o.CARD_COUNTRY,
        o.PAYMENT_ROUTING_STRATEGY,
        o.ORDER_PAYMENT_METHOD,
        o.LAST_CARD_BIN AS CARD_BIN,
        APPF.PROCESSOR_NAME,
        o.ORDER_TOTAL_AMOUNT_USD,
        APPF.PROCESSOR_FEE AS ORDER_PROCESSOR_FEE,
        o.ORDER_TOTAL_AMOUNT_USD*APPF.PROCESSOR_FEE AS ORDER_PROCESSOR_FEE_USD
    FROM 
        VW_ATHENA_ORDER_ o
    JOIN VW_ATHENA_PAYMENT_PROCESSOR_FEE APPF
        ON o.COMMERCE_ID = APPF.COMMERCE_ID AND o.ORDER_ID = APPF.ORDER_ID
    WHERE
        o.ORDER_DATE >= DATEADD(month, -6, CURRENT_DATE())
        AND o.ORDER_APPROVED_INDICATOR = TRUE
        AND o.COMMERCE_ID = '9ea20bdb-5cff-4b10-9c95-9cebf8b6ddb4'
),
GLOBAL_GMV_TOTAL AS (
    SELECT
        SUM(ORDER_TOTAL_AMOUNT_USD) AS TOTAL_GLOBAL_GMV,
        SUM(ORDER_PROCESSOR_FEE_USD) AS TOTAL_GLOBAL_PROCESSOR_FEES
    FROM BASE_DATA
),
ORDER_TABLE AS (
    SELECT 
        orders.ORDER_ID,
        orders.ORDER_DATE,
        orders.ORDER_TIME,
        orders.COMMERCE_ID,
        orders.DATE_MONTH,
        orders.ORDER_PAYMENT_METHOD AS PAYMENT_METHOD,
        orders.ORDER_CITY_NAME,
        orders.CARD_COUNTRY,
        orders.PAYMENT_ROUTING_STRATEGY,
        orders.CARD_BIN,
        orders.PROCESSOR_NAME,
        orders.ORDER_TOTAL_AMOUNT_USD AS PAYMENT_AMOUNT_USD,
        orders.ORDER_PROCESSOR_FEE_USD,
        orders.ORDER_PROCESSOR_FEE,
        orders.ORDER_TOTAL_AMOUNT_USD / global.TOTAL_GLOBAL_GMV AS ORDER_GMV_FRACTION,
        orders.ORDER_PROCESSOR_FEE_USD / global.TOTAL_GLOBAL_GMV AS ORDER_EFFECTIVE_COST_RATE
    FROM BASE_DATA AS orders
    CROSS JOIN GLOBAL_GMV_TOTAL AS global
),

-- ===== STEP 1.4: FINAL DATA PREPARATION FOR ECR ANALYSIS =====
-- PURPOSE: Create the final dataset for ECR distribution analysis
-- QUALITY FILTER: Exclude orders with zero or null ECR (invalid/missing fee data)
-- TEMPORAL FLAG: Add recency indicator for trend analysis capabilities
APPROVED_ECR_TABLE AS (
    SELECT 
        *,  -- All columns from ORDER_TABLE (comprehensive order + ECR metrics)
        -- RECENCY INDICATOR: Flag orders from last month for trend analysis
        -- BUSINESS VALUE: Allows comparison of recent vs historical ECR patterns
        CASE 
            WHEN ORDER_DATE >= DATEADD(month, -1, CURRENT_DATE) THEN 1  -- Recent order
            ELSE 0                                                        -- Historical order
        END AS is_last_month
    FROM ORDER_TABLE                            -- Source: Enriched order data from step 1.3
    WHERE ORDER_PROCESSOR_FEE_USD > 0          -- DATA QUALITY: Only orders with valid ECR data
    -- RATIONALE: Zero ECR orders are either:
    -- 1. Data quality issues (missing fee information)
    -- 2. Special cases (promotional/free transactions)
    -- 3. Would skew distribution analysis toward zero-cost ranges
),
---- =============================================================================
---- STAGE 2: FIRST-LEVEL KNUTH ANALYSIS ON LOGARITHMIC SCALE
---- =============================================================================
---- PURPOSE: Apply Bayesian optimal binning to log-transformed payment amounts
---- WHY LOGARITHMIC: Financial data has extreme right-skew (few very large orders, many small ones)
---- STATISTICAL BENEFIT: Log transformation normalizes the distribution for better binning
---- BUSINESS BENEFIT: Handles payment amounts from $1 to $10,000+ in same analysis
--
---- KNUTH METHOD OVERVIEW:
---- 1. PROBLEM: How many bins should we use for optimal histogram?
---- 2. SOLUTION: Bayesian evidence F(M) finds optimal bin count M*
---- 3. TRADE-OFF: Too few bins = lose detail, too many bins = noise
---- 4. OPTIMIZATION: F(M) = N ln(M) + ln Γ(M/2) - M ln Γ(1/2) - ln Γ((N+M)/2) + Σₖ ln Γ(nₖ + 1/2)
--
---- PRE-AGGREGATION STRATEGY:
---- Instead of evaluating each M candidate on raw data (expensive):
---- 1. Create 20,000 micro-bins once
---- 2. Re-aggregate micro-bins to test different M values
---- 3. Evaluate M from 64 to 4,096 efficiently
--
---- STAGE 2 OUTPUT: Optimal logarithmic bins containing 90% of total ECR

---- ===== STEP 2.1: ALGORITHM CONFIGURATION PARAMETERS =====
---- PURPOSE: Define constants for Knuth method optimization
---- RATIONALE: These parameters balance computational efficiency vs statistical precision
PARAMS AS (
    SELECT 
        -- MICRO-BIN COUNT: Number of uniform divisions in log space
        -- WHY 20,000: Fine enough granularity for precise re-aggregation
        -- TRADE-OFF: Higher = more precise but slower, Lower = faster but less precise
        20000::INT AS U,
        
        -- BIN COUNT SEARCH RANGE: M values to evaluate for optimal binning
        -- MIN = 64: Sufficient resolution for meaningful distribution analysis
        -- MAX = 4096: Upper limit to prevent over-binning and computational explosion
        64::INT    AS M_MIN,           -- Minimum candidate bin count
        4096::INT  AS M_MAX,           -- Maximum candidate bin count
        
        -- GEOMETRIC PROGRESSION STEP: Multiplier for M candidate generation
        -- WHY 1.03: Small enough for thorough search, large enough for efficiency
        -- RESULT: Generates ~300 M candidates between 64 and 4096
        1.03::FLOAT AS M_STEP          -- Step size for geometric grid search
),
-- LOG_STATS: Calculate basic statistics for log-transformed payment amounts
-- Process: Compute count, min, and max of natural logarithm of payment amounts from APPROVED_ECR_TABLE
-- Input: APPROVED_ECR_TABLE (approved orders with ECR data)
-- Output: N (sample size), Y_MIN (minimum log value), Y_MAX (maximum log value) for histogram bounds
LOG_STATS AS (
    SELECT 
        COUNT(*)::INT AS N,
        MIN(LN(PAYMENT_AMOUNT_USD)) AS Y_MIN,
        MAX(LN(PAYMENT_AMOUNT_USD)) AS Y_MAX,
        SUM(ORDER_PROCESSOR_FEE_USD) AS TOTAL_ECR
    FROM APPROVED_ECR_TABLE
    WHERE PAYMENT_AMOUNT_USD > 0
),
-- MGRID: Generate a geometric grid of potential bin counts (M values) to evaluate
-- Process: Create sequence from M_MIN to M_MAX using geometric progression (M_STEP^t)
-- Input: PARAMS (algorithm parameters)
-- Output: Distinct integer M values from 64 to 4096 for Bayesian evaluation
MGRID AS (
    SELECT DISTINCT CAST(ROUND(m_val) AS INT) AS M
    FROM (
        SELECT P.M_MIN * POWER(P.M_STEP, SEQ4() - 1) AS m_val
        FROM PARAMS P, TABLE(GENERATOR(ROWCOUNT=>1000))
    )
    WHERE m_val BETWEEN (SELECT M_MIN FROM PARAMS) AND (SELECT M_MAX FROM PARAMS)
),
-- MICRO_BINS: Pre-aggregation step 1 - Assign each payment amount to one of U micro-bins
-- Process: Transform each log(payment_amount) to micro-bin index u using uniform division
-- Input: APPROVED_ECR_TABLE (approved orders with ECR), LOG_STATS (log bounds), PARAMS (U=20000)
-- Output: Each approved order mapped to micro-bin index u ∈ [0, U-1] with ECR value
MICRO_BINS AS (
    SELECT 
        LEAST(GREATEST(
            -- Use CEIL instead of FLOOR to assign boundary values to the higher bin
            CAST(CEIL( (LN(T.PAYMENT_AMOUNT_USD) - S.Y_MIN) / ((S.Y_MAX - S.Y_MIN) / P.U) ) - 1 AS INT)
        , 0), P.U-1) AS u,
        T.ORDER_PROCESSOR_FEE_USD AS ecr_value
    FROM APPROVED_ECR_TABLE T
    JOIN LOG_STATS S ON TRUE
    JOIN PARAMS P    ON TRUE
    WHERE T.PAYMENT_AMOUNT_USD > 0
),
-- MICRO_COUNTS: Count observations and sum ECR values in each micro-bin
-- Process: Aggregate MICRO_BINS to count orders and sum ECR values in each micro-bin u
-- Input: MICRO_BINS (order-to-micro-bin mappings with ECR values)
-- Output: Order count c and ECR sum ecr_total for each micro-bin u (pre-aggregated data for efficiency)
MICRO_COUNTS AS (
    SELECT u, COUNT(*)::INT AS c, SUM(ecr_value) AS ecr_total
    FROM MICRO_BINS
    GROUP BY u
),
-- ===== STEP 2.6: MACRO-BIN RE-AGGREGATION FOR ALL M CANDIDATES =====
-- PURPOSE: For each M candidate, map micro-bins to M macro-bins and aggregate
-- EFFICIENCY: Test 300 different M values without re-scanning original data
-- MAPPING FORMULA: macro_bin_k = floor(micro_bin_u * M / U)
MACRO_COUNTS AS (
    SELECT 
        M.M,                                    -- Candidate bin count (64 to 4096)
        
        -- === MICRO-TO-MACRO BIN MAPPING ===
        -- FORMULA: k = floor(u * M / U)
        -- LOGIC: Proportionally map U=20,000 micro-bins to M macro-bins
        -- EXAMPLE: If M=100, micro-bins 0-199 → macro-bin 0, 200-399 → macro-bin 1, etc.
        CAST(FLOOR( (MC.u * M.M) / P.U ) AS INT) AS k,
        
        -- === AGGREGATED METRICS PER MACRO-BIN ===
        -- ORDER COUNT: Total orders in macro-bin k for bin count M
        -- PURPOSE: Traditional histogram frequency analysis
        SUM(MC.c)::INT AS n_k,
        
        -- ECR TOTAL: Total processor fees in macro-bin k for bin count M
        -- PURPOSE: ECR-weighted histogram analysis (our primary focus)
        -- BUSINESS MEANING: Processing cost concentration in this payment range
        SUM(MC.ecr_total) AS ecr_k
        
    FROM MICRO_COUNTS MC                    -- Source: Pre-aggregated micro-bin data from step 2.5
    CROSS JOIN MGRID M                      -- Cross join: Test all M candidates from step 2.3
    JOIN PARAMS P ON TRUE                   -- Cross join: Get U parameter for mapping formula
    GROUP BY M.M, CAST(FLOOR( (MC.u * M.M) / P.U ) AS INT)
    -- RESULT: For each M, produces M rows (one per macro-bin k)
),

/* ---- Stirling's Approximation for lnGamma(x):
   ln Γ(x) ≈ (x-0.5) ln x - x + 0.5 ln(2π) + 1/(12x) - 1/(360x^3)
   (This approximation is sufficient for our M range; empty bins are handled separately)
*/
-- ===== STEP 2.7: MATHEMATICAL CONSTANTS FOR GAMMA FUNCTION APPROXIMATION =====
-- PURPOSE: Pre-compute constants needed for Stirling's approximation of ln(Γ(x))
-- WHY NEEDED: Bayesian evidence F(M) requires gamma function evaluations
-- OPTIMIZATION: Calculate once, reuse for all M candidates
STIRLING_CONST AS (
    SELECT 
        -- CONSTANT 1: ln(2π) for Stirling's approximation formula
        -- USAGE: Appears in every Stirling approximation calculation
        LN(2*PI()) AS LN_2PI,
        
        -- CONSTANT 2: ln(Γ(1/2)) = 0.5 * ln(π) - known mathematical constant
        -- PURPOSE: Used in F(M) formula for the M * ln(Γ(1/2)) term
        -- MATHEMATICAL FACT: Γ(1/2) = √π, so ln(Γ(1/2)) = 0.5 * ln(π)
        0.5*LN(PI()) AS LN_GAMMA_HALF_CONST
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
-- ===== STEP 2.9: BAYESIAN EVIDENCE F(M) CALCULATION =====
-- PURPOSE: Calculate complete Bayesian evidence score for each candidate M
-- MATHEMATICAL FORMULA: F(M) = N ln(M) + ln(Γ(M/2)) - M ln(Γ(1/2)) - ln(Γ((N+M)/2)) + Σₖ ln(Γ(nₖ+1/2))
-- BUSINESS GOAL: Find M* that maximizes F(M) = optimal trade-off between model complexity and fit
KNUTH_SCORES AS (
    SELECT 
        T.M,                                -- Candidate bin count being scored
        
        -- === BAYESIAN EVIDENCE F(M) CALCULATION ===
        -- TERM 1: N * ln(M) - penalty for model complexity (more bins = higher penalty)
        (S.N * LN(T.M))
        
        -- TERM 2: + ln(Γ(M/2)) - prior for number of bins
        -- STIRLING APPROXIMATION: For large M/2 values
        + (
            (
                -- (x-0.5) * ln(x) where x = M/2
                (( (T.M/2.0) - 0.5) * LN(T.M/2.0))
                - (T.M/2.0)                              -- -x
                + 0.5 * C.LN_2PI                         -- +0.5 * ln(2π)
                + (1.0 / (12.0 * (T.M/2.0)))             -- +1/(12x)
                - (1.0 / (360.0 * POW(T.M/2.0, 3)))     -- -1/(360x³)
            )
        )
        
        -- TERM 3: - M * ln(Γ(1/2)) - normalization constant
        -- MATHEMATICAL FACT: Γ(1/2) = √π, so this term = -M * 0.5 * ln(π)
        - (T.M * C.LN_GAMMA_HALF_CONST)
        
        -- TERM 4: - ln(Γ((N+M)/2)) - normalization for total sample + bins
        -- STIRLING APPROXIMATION: For large (N+M)/2 values
        - (
            (
                -- (x-0.5) * ln(x) where x = (N+M)/2
                (( ((S.N + T.M)/2.0) - 0.5) * LN( (S.N + T.M)/2.0 ))
                - ((S.N + T.M)/2.0)                     -- -x
                + 0.5 * C.LN_2PI                         -- +0.5 * ln(2π)
                + (1.0 / (12.0 * ((S.N + T.M)/2.0)))     -- +1/(12x)
                - (1.0 / (360.0 * POW((S.N + T.M)/2.0, 3))) -- -1/(360x³)
            )
        )
        
        -- TERM 5: + Σₖ ln(Γ(nₖ+1/2)) - reward for data fit
        -- INCLUDES EMPTY BINS: Empty bins contribute ln(Γ(1/2)) each
        -- TOTAL = sum_for_nonempty_bins + (empty_bin_count * ln(Γ(1/2)))
        + (T.sum_lngamma_nonzero + (T.M - T.nonzero_bins) * C.LN_GAMMA_HALF_CONST)
        
        AS F_SCORE                              -- Final Bayesian evidence score
        
    FROM TERMS T                            -- Source: Gamma function terms from step 2.8
    JOIN LOG_STATS S ON TRUE                -- Cross join: Get sample size N
    JOIN STIRLING_CONST C ON TRUE           -- Cross join: Get mathematical constants
    -- RESULT: One F_SCORE per candidate M - higher scores indicate better histograms
),
-- ===== STEP 2.10: OPTIMAL BIN COUNT SELECTION =====
-- PURPOSE: Select M* that maximizes Bayesian evidence F(M)
-- OPTIMIZATION GOAL: argmax F(M) = best balance between model complexity and data fit
-- RESULT: Single optimal bin count for Stage 2 logarithmic histogram
BEST_M AS (
    SELECT 
        -- OPTIMAL BIN COUNT: M* with highest F(M) score
        -- BUSINESS MEANING: Best number of bins to analyze ECR distribution in log space
        -- TYPICAL RANGE: Usually between 200-800 bins for financial data
        M AS optimal_bin_count
    FROM KNUTH_SCORES                   -- Source: F(M) scores from step 2.9
    ORDER BY F_SCORE DESC              -- Sort by Bayesian evidence (highest first)
    LIMIT 1                            -- Select only the best M*
    -- RESULT: Single row with optimal_bin_count for histogram construction
),

/* ===== HISTOGRAM CONSTRUCTION WITH OPTIMAL BIN COUNT =====
   - Calculate bin edges in log space using M* bins, then transform back to USD
   - Count frequencies per bin using the same micro-to-macro bin mapping (u -> k) with optimal M*
*/
-- ===== STEP 2.11: FINAL HISTOGRAM BIN AGGREGATION =====
-- PURPOSE: Re-aggregate micro-bins into optimal M* macro-bins using best bin count
-- MAPPING FORMULA: k = floor(u * M* / U) - maps micro-bin u to final bin k
-- AGGREGATION: Sum order frequencies and ECR totals within each final bin
FINAL_COUNTS AS (
    SELECT 
        -- BIN INDEX CALCULATION: Map micro-bin u to final bin k
        -- FORMULA: k = floor(u * optimal_M / total_microbins)
        -- PURPOSE: Distribute U micro-bins evenly across M* final bins
        CAST(FLOOR( (MC.u * B.optimal_bin_count) / P.U ) AS INT) AS k,
        
        -- FREQUENCY AGGREGATION: Total orders in this final bin k
        -- SOURCE: Sum frequencies from all micro-bins that map to bin k
        SUM(MC.c) AS freq,
        
        -- ECR AGGREGATION: Total ECR in this final bin k
        -- SOURCE: Sum ECR totals from all micro-bins that map to bin k
        -- BUSINESS MEANING: Total processing cost concentration in this payment range
        SUM(MC.ecr_total) AS ecr_total
        
    FROM MICRO_COUNTS MC                -- Source: Micro-bin data from step 2.5
    JOIN BEST_M B ON TRUE               -- Cross join: Get optimal bin count M*
    JOIN PARAMS P ON TRUE               -- Cross join: Get total micro-bins U
    GROUP BY CAST(FLOOR( (MC.u * B.optimal_bin_count) / P.U ) AS INT)
    -- RESULT: Aggregated histogram bins with optimal binning using M*
),
-- ===== STEP 2.12: COMPLETE BIN INDEX GENERATION =====
-- PURPOSE: Generate all possible bin indices k = 0, 1, 2, ..., M*-1
-- WHY NEEDED: Ensure complete histogram coverage, including empty bins
-- METHOD: Use Snowflake GENERATOR function to create sequence
BINS_ALL AS (
    SELECT 
        -- BIN INDEX: Sequential bin number from 0 to M*-1
        -- MATHEMATICAL RANGE: k ∈ {0, 1, 2, ..., optimal_bin_count-1}
        -- PURPOSE: Ensures all bins are represented, even if empty
        ROW_NUMBER() OVER (ORDER BY seq4()) - 1 AS k
        
    FROM TABLE(GENERATOR(ROWCOUNT=>100000))   -- Generate large sequence (Snowflake function)
    JOIN BEST_M B ON TRUE                     -- Cross join: Get optimal bin count
    QUALIFY k < B.optimal_bin_count           -- Filter: Keep only k < M*
    -- RESULT: Complete sequence of bin indices for optimal histogram
),
-- ===== STEP 2.13: LOG-SPACE BIN EDGE CALCULATION =====
-- PURPOSE: Create equal-width bins in logarithmic space using optimal M*
-- MATHEMATICAL APPROACH: Divide [Y_MIN, Y_MAX] into M* equal intervals
-- BENEFIT: Equal width in log space = proportional width in USD space
LOG_EDGES AS (
    SELECT 
        BA.k,                               -- Bin index (0 to M*-1)
        
        -- LEFT EDGE IN LOG SPACE: y_left = Y_MIN + (range * k/M*)
        -- FORMULA: Linear interpolation from Y_MIN to Y_MAX
        -- MATHEMATICAL MEANING: Start of bin k in ln(USD) space
        S.Y_MIN + (S.Y_MAX - S.Y_MIN) * (BA.k / B.optimal_bin_count) AS y_left,
        
        -- RIGHT EDGE IN LOG SPACE: y_right = Y_MIN + (range * (k+1)/M*)
        -- FORMULA: Linear interpolation for end of bin k
        -- MATHEMATICAL MEANING: End of bin k in ln(USD) space
        S.Y_MIN + (S.Y_MAX - S.Y_MIN) * ((BA.k+1.0) / B.optimal_bin_count) AS y_right
        
    FROM BINS_ALL BA                    -- Source: Complete bin indices from step 2.12
    JOIN BEST_M B ON TRUE               -- Cross join: Get optimal bin count M*
    JOIN LOG_STATS S ON TRUE            -- Cross join: Get log space bounds [Y_MIN, Y_MAX]
    -- RESULT: Log-space boundaries for each bin k
),
-- ===== STEP 2.14: USD SPACE BIN EDGE TRANSFORMATION =====
-- PURPOSE: Convert logarithmic bin edges back to USD payment amounts
-- TRANSFORMATION: exp(y_left) and exp(y_right) to get USD boundaries
-- BOUNDARY HANDLING: Use exact data min/max for first and last bins
USD_BINS AS (
    SELECT 
        L.k,                                -- Bin index (0 to M*-1)
        
        -- LEFT EDGE IN USD: Convert y_left back to payment amount
        -- SPECIAL CASE: First bin (k=0) uses exact minimum from data
        -- REASON: Avoid rounding errors at data boundaries
        CASE WHEN L.k = 0 THEN 
            (SELECT MIN(PAYMENT_AMOUNT_USD) FROM APPROVED_ECR_TABLE WHERE PAYMENT_AMOUNT_USD > 0)
        ELSE EXP(L.y_left) END AS edge_left_usd,
        
        -- RIGHT EDGE IN USD: Convert y_right back to payment amount  
        -- SPECIAL CASE: Last bin (k=M*-1) uses exact maximum from data
        -- REASON: Ensure complete data coverage without gaps
        CASE WHEN L.k = B.optimal_bin_count - 1 THEN 
            (SELECT MAX(PAYMENT_AMOUNT_USD) FROM APPROVED_ECR_TABLE)
        ELSE EXP(L.y_right) END AS edge_right_usd,
        
        -- BIN CENTER IN USD: Geometric mean of log-space boundaries
        -- FORMULA: exp((y_left + y_right)/2) = √(left_usd * right_usd)
        -- BUSINESS MEANING: Representative payment amount for this bin
        EXP( (L.y_left + L.y_right)/2.0 ) AS bin_center_usd
        
    FROM LOG_EDGES L                    -- Source: Log-space boundaries from step 2.13
    JOIN BEST_M B ON TRUE               -- Cross join: Get optimal bin count for boundary detection
    -- RESULT: USD bin definitions with exact data boundary alignment
),
-- ===== STEP 2.15: COMPLETE HISTOGRAM CONSTRUCTION =====
-- PURPOSE: Combine bin definitions with actual frequencies and ECR totals
-- METHOD: LEFT JOIN to include all bins (even empty ones) with zero-filled missing data
-- RESULT: Complete histogram ready for Stage 3 filtering analysis
HIST AS (
    SELECT 
        U.k,                                -- Bin index (0 to M*-1)
        
        -- BIN BOUNDARIES IN USD: Transformed from log space
        -- BUSINESS MEANING: Payment amount ranges for strategic analysis
        U.edge_left_usd,                   -- Left boundary in USD
        U.edge_right_usd,                  -- Right boundary in USD
        U.bin_center_usd,                  -- Representative amount for this bin
        
        -- ORDER FREQUENCY: Number of approved orders in this payment range
        -- ZERO-FILL: Empty bins get frequency = 0 (important for complete histogram)
        COALESCE(F.freq, 0) AS frequency,
        
        -- ECR CONCENTRATION: Total effective cost rate in this payment range
        -- ZERO-FILL: Empty bins get ecr_total = 0
        -- BUSINESS MEANING: Total processing cost burden for this payment amount range
        COALESCE(F.ecr_total, 0) AS ecr_total
        
    FROM USD_BINS U                     -- Source: USD bin definitions from step 2.14
    LEFT JOIN FINAL_COUNTS F USING (k)  -- Left join: Include all bins, fill missing with NULL
    -- RESULT: Complete histogram with M* bins, ready for filtering analysis
),
-- =============================================================================
-- ===== STAGE 3: FIRST-LEVEL FILTERING BASED ON LOGARITHMIC ANALYSIS =====
-- =============================================================================
-- PURPOSE: Apply Pareto-style filtering to focus on highest ECR concentration bins
-- STRATEGY: Rank bins by ECR total and select top 90% to eliminate outliers
-- BUSINESS GOAL: Focus analysis on payment ranges with highest processing costs

-- ===== STEP 3.1: ECR-BASED BIN RANKING AND CUMULATIVE ANALYSIS =====
-- PURPOSE: Rank histogram bins by ECR concentration and calculate cumulative metrics
-- FILTERING LOGIC: Select bins contributing to top 90% of total ECR
-- BUSINESS RATIONALE: Focus on payment ranges driving majority of processing costs
LOG_FIRST_FILTER AS (
    SELECT 
        -- BIN RANKING: Order bins by ECR concentration (highest cost ranges first)
        -- BUSINESS MEANING: Rank payment ranges by their cost impact
        ROW_NUMBER() OVER (ORDER BY ecr_total DESC) AS bin_order,
        
        -- BIN IDENTIFICATION: Convert 0-indexed k to 1-indexed bin_number
        -- DISPLAY PURPOSE: More intuitive bin numbering for business users
        (k + 1) AS bin_number,
        
        -- USD BOUNDARIES: Payment amount range boundaries for this bin
        -- STRATEGIC USE: Define specific payment ranges for optimization
        edge_left_usd AS bin_start,            -- Lower payment amount boundary
        edge_right_usd AS bin_end,             -- Upper payment amount boundary
        
        -- BIN CENTERS: Representative payment amounts for this range
        bin_center_usd AS geometric_bin_center, -- Geometric mean (better for log-distributed data)
        (edge_left_usd + edge_right_usd)/2.0 AS bin_center, -- Arithmetic mean (linear center)
        
        -- BIN WIDTH: Payment amount range span
        -- INSIGHT: Wider bins in higher payment ranges due to log-scale binning
        (edge_right_usd - edge_left_usd) AS bin_width,
        
        -- CUMULATIVE ECR ANALYSIS: Running total of ECR as we move down ranked bins
        -- CALCULATION: Sum ECR from highest-cost bins to current bin
        -- BUSINESS USE: Track how much of total cost burden is covered
        SUM(ecr_total) OVER (ORDER BY ecr_total DESC) AS cumulative_ecr,
        
        -- CUMULATIVE ECR PERCENTAGE: What fraction of total ECR is covered so far
        -- FORMULA: cumulative_ecr / total_ecr_across_all_approved_orders
        -- THRESHOLD LOGIC: Will be used to identify 90% cutoff point
        SUM(ecr_total) OVER (ORDER BY ecr_total DESC) / (SELECT TOTAL_ECR FROM LOG_STATS) AS cumulative_ecr_percentage,
        
        -- RANGE OF INTEREST FLAG: Mark bins that contribute to top 90% of ECR
        -- LOGIC: 1 if this bin is part of the top 90% ECR concentration, 0 otherwise
        -- STRATEGIC PURPOSE: Focus subsequent analysis on highest-impact payment ranges
        CASE 
            WHEN SUM(ecr_total) OVER (ORDER BY ecr_total DESC) / (SELECT TOTAL_ECR FROM LOG_STATS) < 0.9 THEN 1
            ELSE 0
        END AS in_range_of_interest,
        
        -- BIN METRICS: Pass through frequency and ECR data for analysis
        frequency,                              -- Number of orders in this payment range
        ecr_total                              -- Total processing cost for this payment range
        
    FROM HIST                               -- Source: Complete histogram from step 2.15
    ORDER BY ecr_total DESC                 -- Sort by ECR total (highest cost impact first)
    -- RESULT: Ranked bins with cumulative ECR analysis and filtering flags
),
-- =============================================================================
-- ===== STAGE 4: ORDER FILTERING BASED ON LOG-SCALE ECR ANALYSIS =====
-- =============================================================================
-- PURPOSE: Apply first-level filter to focus on orders in highest ECR concentration ranges
-- FILTERING STRATEGY: Keep only orders in bins representing top 90% of ECR
-- BUSINESS IMPACT: Eliminates outliers and rare payment amounts, focuses on cost drivers

-- ===== STEP 4.1: APPROVED ORDER FILTERING BY ECR CONCENTRATION =====
-- PURPOSE: Filter original orders to retain only those in high-ECR payment ranges
-- METHOD: Join orders with LOG_FIRST_FILTER bins marked as in_range_of_interest = 1
-- RESULT: ~90% of total ECR retained while removing noise from edge cases
FILTERED_APPROVED_ECR_TABLE AS (
    SELECT 
        -- PASS-THROUGH: All order attributes from original approved dataset
        -- FILTERING APPLIED: Only orders within high-ECR payment ranges included
        -- BUSINESS VALUE: Focus analysis on strategically important transactions
        APPROVED_ECR_TABLE.*
        
    FROM APPROVED_ECR_TABLE                 -- Source: All approved orders from step 1.4
    
    -- JOIN CONDITION: Match orders to their corresponding histogram bins
    -- RANGE CHECK: PAYMENT_AMOUNT_USD must fall within [bin_start, bin_end]
    JOIN LOG_FIRST_FILTER ON APPROVED_ECR_TABLE.PAYMENT_AMOUNT_USD BETWEEN LOG_FIRST_FILTER.bin_start AND LOG_FIRST_FILTER.bin_end
    
    -- FILTER CONDITION: Only include bins marked as high-ECR concentration
    -- THRESHOLD: in_range_of_interest = 1 means this bin contributes to top 90% ECR
    WHERE LOG_FIRST_FILTER.in_range_of_interest = 1
    
    -- RESULT: Filtered dataset with ~90% of ECR but fewer outlier transactions
),

-- =============================================================================
-- ===== STAGE 5: SECOND-LEVEL KNUTH ANALYSIS ON LINEAR SCALE =====
-- =============================================================================
-- PURPOSE: Apply refined Knuth method on filtered data using linear USD amounts
-- SCOPE: Re-analyze the ~90% ECR subset from Stage 4 using linear-scale binning
-- BUSINESS GOAL: Find optimal linear-scale intervals for final strategic insights
-- EXPECTED RESULT: ~81% of original data (90% of the filtered 90%)

/* ===== LINEAR-SCALE KNUTH METHOD WITH PRE-AGGREGATION ===== */
-- METHODOLOGY: Same Bayesian optimization approach as Stage 2, but on linear USD scale
-- KEY DIFFERENCE: No logarithmic transformation - direct USD amount binning
-- ADVANTAGE: More intuitive payment amount intervals for business decisions

-- ===== STEP 5.1: LINEAR KNUTH METHOD PARAMETER CONFIGURATION =====
-- PURPOSE: Configure parameters for second-stage Bayesian analysis on linear USD scale
-- CONSISTENCY: Uses same parameter values as Stage 2 for comparable analysis
-- SCOPE: Applied to filtered dataset (~90% of original ECR)
PARAMS_2 AS (
    SELECT 
        -- MICRO-BIN COUNT: Number of fine-grained bins for pre-aggregation
        -- VALUE: 20000 micro-bins in linear USD space
        -- PURPOSE: High resolution for accurate bin assignment before macro-aggregation
        20000::INT AS U,
        
        -- BIN COUNT RANGE: Search space for optimal macro-bin count
        -- MINIMUM: 64 bins (coarse analysis)
        -- MAXIMUM: 4096 bins (fine-grained analysis)
        64::INT AS M_MIN,
        4096::INT AS M_MAX,
        
        -- GEOMETRIC PROGRESSION: Step size for M candidate generation
        -- VALUE: 1.03 ensures comprehensive coverage of bin count space
        -- RESULT: ~300 candidate M values between M_MIN and M_MAX
        1.03::FLOAT AS M_STEP
),
-- ===== STEP 5.2: FILTERED DATASET STATISTICAL ANALYSIS =====
-- PURPOSE: Calculate key statistics for linear-scale binning on filtered data
-- SCOPE: Analyze payment amounts from Stage 4 filtered dataset (~90% ECR)
-- NO LOG TRANSFORMATION: Direct USD amounts for intuitive business intervals
STATS_2 AS (
    SELECT 
        -- SAMPLE SIZE: Number of orders in filtered dataset
        -- BUSINESS CONTEXT: ~90% of original ECR concentrated in this subset
        -- USAGE: N will be used in Bayesian F(M) formula
        COUNT(*)::INT AS N,
        
        -- LINEAR SCALE BOUNDS: USD payment amount range for filtered data
        -- Y_MIN: Minimum payment amount in filtered dataset
        -- Y_MAX: Maximum payment amount in filtered dataset
        -- PURPOSE: Define binning bounds for linear-scale histogram
        MIN(PAYMENT_AMOUNT_USD) AS Y_MIN,
        MAX(PAYMENT_AMOUNT_USD) AS Y_MAX,
        
        -- TOTAL ECR: Sum of all processing costs in filtered dataset
        -- BUSINESS USE: Denominator for calculating ECR concentration percentages
        -- VALIDATION: Should be ~90% of original TOTAL_ECR from LOG_STATS
        SUM(ORDER_PROCESSOR_FEE_USD) AS TOTAL_ECR
        
    FROM FILTERED_APPROVED_ECR_TABLE    -- Source: Stage 4 filtered orders
    WHERE PAYMENT_AMOUNT_USD > 0        -- Exclude invalid payment amounts
    -- RESULT: Statistical foundation for linear-scale Knuth analysis
),
-- ===== STEP 5.3: BIN COUNT CANDIDATE GENERATION =====
-- PURPOSE: Create geometric grid of M values for linear-scale Bayesian optimization
-- METHODOLOGY: Same geometric progression as Stage 2, applied to filtered data
-- SEARCH SPACE: Comprehensive evaluation from coarse to fine-grained binning
MGRID_2 AS (
    SELECT DISTINCT CAST(ROUND(m_val) AS INT) AS M
    FROM (
        -- GEOMETRIC SEQUENCE: M_MIN * (M_STEP^i) for i = 0, 1, 2, ...
        -- FORMULA: 64 * (1.03^i) generates candidates from 64 to 4096
        -- RESULT: ~300 candidate M values with logarithmic spacing
        SELECT P.M_MIN * POWER(P.M_STEP, SEQ4()) AS m_val
        FROM PARAMS_2 P, TABLE(GENERATOR(ROWCOUNT=>300))
    )
    -- BOUNDS CHECK: Keep only M values within specified range
    -- PURPOSE: Ensure all candidates are computationally feasible
    WHERE m_val BETWEEN (SELECT M_MIN FROM PARAMS_2) AND (SELECT M_MAX FROM PARAMS_2)
    -- RESULT: Candidate M values for second-stage F(M) optimization
),
-- ===== STEP 5.4: LINEAR MICRO-BIN ASSIGNMENT =====
-- PURPOSE: Map each filtered order to fine-grained micro-bins in linear USD space
-- KEY DIFFERENCE: No log transformation - direct linear division of payment amounts
-- COMPUTATIONAL STRATEGY: Pre-aggregate into U=20000 micro-bins for efficiency
MICRO_BINS_2 AS (
    SELECT 
        -- MICRO-BIN INDEX CALCULATION: Map USD amount to micro-bin u
        -- FORMULA: u = ceil((amount - Y_MIN) / bin_width) - 1
        -- WHERE: bin_width = (Y_MAX - Y_MIN) / U
        -- BOUNDARY HANDLING: LEAST/GREATEST ensure u ∈ [0, U-1]
        LEAST(GREATEST(
            -- LINEAR BINNING: Equal-width bins in USD space
            -- CEIL FUNCTION: Boundary values assigned to higher bin (consistency with Stage 2)
            -- ADJUSTMENT: -1 converts from 1-indexed to 0-indexed
            CAST(CEIL( (T.PAYMENT_AMOUNT_USD - S.Y_MIN) / ((S.Y_MAX - S.Y_MIN) / P.U) ) - 1 AS INT)
        , 0), P.U-1) AS u,
        
        -- ECR VALUE: Processing cost for this order
        -- PURPOSE: Will be aggregated by micro-bin for ECR concentration analysis
        -- BUSINESS MEANING: Individual order's contribution to processing costs
        T.ORDER_PROCESSOR_FEE_USD AS ecr_value
        
    FROM FILTERED_APPROVED_ECR_TABLE T  -- Source: Stage 4 filtered orders (~90% ECR)
    JOIN STATS_2 S ON TRUE              -- Cross join: Get linear scale bounds
    JOIN PARAMS_2 P ON TRUE             -- Cross join: Get micro-bin count U
    WHERE T.PAYMENT_AMOUNT_USD > 0      -- Exclude invalid amounts
    -- RESULT: Each filtered order mapped to linear micro-bin with ECR value
),
-- ===== STEP 5.5: LINEAR MICRO-BIN AGGREGATION =====
-- PURPOSE: Aggregate filtered orders and ECR within each linear micro-bin
-- EFFICIENCY: Pre-aggregation reduces computational load for subsequent M evaluations
-- SCOPE: Applied to ~90% ECR subset from Stage 4 filtering
MICRO_COUNTS_2 AS (
    SELECT 
        u,                              -- Micro-bin index (0 to U-1)
        
        -- ORDER COUNT: Number of filtered orders in this linear micro-bin
        -- BUSINESS CONTEXT: Frequency distribution in linear USD space
        COUNT(*)::INT AS c,
        
        -- ECR AGGREGATION: Total processing costs in this linear micro-bin
        -- PURPOSE: Measure cost concentration in linear payment amount ranges
        -- USAGE: Will be re-aggregated into macro-bins for different M values
        SUM(ecr_value) AS ecr_total
        
    FROM MICRO_BINS_2                   -- Source: Order-to-micro-bin mappings from step 5.4
    GROUP BY u                          -- Aggregate by micro-bin index
    -- RESULT: Pre-aggregated linear micro-bin data for efficient macro-bin evaluation
),
-- ===== STEP 5.6: LINEAR MACRO-BIN AGGREGATION FOR ALL CANDIDATE M VALUES =====
-- PURPOSE: Re-aggregate linear micro-bins into macro-bins for each candidate M
-- MAPPING FORMULA: k = floor(u * M / U) - distribute U micro-bins across M macro-bins
-- COMPUTATIONAL EFFICIENCY: Evaluate all M candidates simultaneously using cross join
MACRO_COUNTS_2 AS (
    SELECT 
        M.M,                                -- Candidate bin count for linear analysis
        
        -- MACRO-BIN INDEX: Map micro-bin u to macro-bin k for this M
        -- FORMULA: k = floor(u * M / U) ensures even distribution
        -- RANGE: k ∈ [0, M-1] for each candidate M
        CAST(FLOOR( (MC.u * M.M) / P.U ) AS INT) AS k,
        
        -- ORDER COUNT AGGREGATION: Sum orders from micro-bins mapping to macro-bin k
        -- BUSINESS MEANING: Number of filtered orders in this linear payment range
        SUM(MC.c)::INT AS n_k,
        
        -- ECR AGGREGATION: Sum ECR from micro-bins mapping to macro-bin k
        -- BUSINESS MEANING: Total processing costs in this linear payment range
        -- USAGE: Key metric for identifying high-cost linear intervals
        SUM(MC.ecr_total) AS ecr_k
        
    FROM MICRO_COUNTS_2 MC              -- Source: Linear micro-bin aggregations from step 5.5
    CROSS JOIN MGRID_2 M                -- Cross join: Evaluate all candidate M values
    JOIN PARAMS_2 P ON TRUE             -- Cross join: Get micro-bin count U
    GROUP BY M.M, CAST(FLOOR( (MC.u * M.M) / P.U ) AS INT)
    -- RESULT: Macro-bin frequencies and ECR totals for each (M,k) combination
),
-- ===== STEP 5.7: LINEAR SCALE GAMMA FUNCTION TERMS CALCULATION =====
-- PURPOSE: Calculate Σₖ ln(Γ(nₖ + 1/2)) for each candidate M in linear analysis
-- METHODOLOGY: Same Stirling approximation as Stage 2, applied to filtered linear data
-- SCOPE: Second-stage Bayesian optimization on ~90% ECR subset
TERMS_2 AS (
    SELECT 
        MC.M,                           -- Candidate bin count for linear analysis
        
        -- === STIRLING APPROXIMATION FOR LINEAR MACRO-BINS ===
        -- SAME FORMULA: ln(Γ(n_k + 0.5)) using Stirling's approximation
        -- APPLIED TO: Linear macro-bins from filtered ECR dataset
        SUM(
            -- GAMMA FUNCTION: ln(Γ(n_k + 0.5)) where n_k = orders in linear bin k
            (
                -- TERM 1: (x-0.5) * ln(x) where x = n_k + 0.5
                (( (n_k + 0.5) - 0.5) * LN(n_k + 0.5))
                
                -- TERM 2: -x = -(n_k + 0.5)
                - (n_k + 0.5)
                
                -- TERM 3: +0.5 * ln(2π) - Stirling constant
                + 0.5 * (SELECT LN_2PI FROM STIRLING_CONST)
                
                -- TERM 4: +1/(12x) - first-order correction
                + (1.0 / (12.0 * (n_k + 0.5)))
                
                -- TERM 5: -1/(360x³) - second-order correction
                - (1.0 / (360.0 * POW(n_k + 0.5, 3)))
            )
        ) AS sum_lngamma_nonzero,
        
        -- NON-EMPTY BIN COUNT: Track bins with at least one order
        -- PURPOSE: Handle empty bins in F(M) calculation
        COUNT(*) AS nonzero_bins
        
    FROM MACRO_COUNTS_2 MC              -- Source: Linear macro-bin frequencies from step 5.6
    GROUP BY MC.M                       -- Aggregate by candidate M value
    -- RESULT: Gamma function terms for linear-scale F(M) optimization
),
-- ===== STEP 5.8: LINEAR SCALE BAYESIAN EVIDENCE F(M) CALCULATION =====
-- PURPOSE: Calculate complete F(M) scores for linear-scale bin count optimization
-- FORMULA: Same Bayesian evidence formula as Stage 2, applied to filtered linear data
-- OPTIMIZATION GOAL: Find optimal M* for linear USD intervals
KNUTH_SCORES_2 AS (
    SELECT 
        T.M,                                -- Candidate bin count for linear analysis
        
        -- === BAYESIAN EVIDENCE F(M) FOR LINEAR SCALE ===
        -- TERM 1: N * ln(M) - model complexity penalty
        -- CONTEXT: N = filtered sample size (~90% of original orders)
        (S.N * LN(T.M))
        
        -- TERM 2: + ln(Γ(M/2)) - prior for number of bins
        -- STIRLING APPROXIMATION: Same formula as Stage 2
        + (
            (
                -- (x-0.5) * ln(x) where x = M/2
                (( (T.M/2.0) - 0.5) * LN(T.M/2.0))
                - (T.M/2.0)                              -- -x
                + 0.5 * C.LN_2PI                         -- +0.5 * ln(2π)
                + (1.0 / (12.0 * (T.M/2.0)))             -- +1/(12x)
                - (1.0 / (360.0 * POW(T.M/2.0, 3)))     -- -1/(360x³)
            )
        )
        
        -- TERM 3: - M * ln(Γ(1/2)) - normalization constant
        -- MATHEMATICAL CONSTANT: Same as Stage 2
        - (T.M * C.LN_GAMMA_HALF_CONST)
        
        -- TERM 4: - ln(Γ((N+M)/2)) - normalization for total filtered sample + bins
        -- STIRLING APPROXIMATION: Applied to filtered sample size
        - (
            (
                -- (x-0.5) * ln(x) where x = (filtered_N + M)/2
                (( ((S.N + T.M)/2.0) - 0.5) * LN( (S.N + T.M)/2.0 ))
                - ((S.N + T.M)/2.0)                     -- -x
                + 0.5 * C.LN_2PI                         -- +0.5 * ln(2π)
                + (1.0 / (12.0 * ((S.N + T.M)/2.0)))     -- +1/(12x)
                - (1.0 / (360.0 * POW((S.N + T.M)/2.0, 3))) -- -1/(360x³)
            )
        )
        
        -- TERM 5: + Σₖ ln(Γ(nₖ+1/2)) - data fit reward for linear bins
        -- INCLUDES EMPTY BINS: Same treatment as Stage 2
        + (T.sum_lngamma_nonzero + (T.M - T.nonzero_bins) * C.LN_GAMMA_HALF_CONST)
        
        AS F_SCORE                              -- Bayesian evidence for linear-scale binning
        
    FROM TERMS_2 T                          -- Source: Linear gamma terms from step 5.7
    JOIN STATS_2 S ON TRUE                  -- Cross join: Get filtered sample statistics
    JOIN STIRLING_CONST C ON TRUE           -- Cross join: Get mathematical constants
    -- RESULT: F(M) scores for linear-scale optimization on filtered data
),
-- ===== STEP 5.9: OPTIMAL LINEAR BIN COUNT SELECTION =====
-- PURPOSE: Select M* that maximizes F(M) for linear-scale analysis
-- OPTIMIZATION: argmax F(M) on filtered dataset for final interval detection
-- BUSINESS GOAL: Find optimal linear binning for strategic cost insights
BEST_M_2 AS (
    SELECT 
        -- OPTIMAL LINEAR BIN COUNT: M* with highest F(M) for linear analysis
        -- SCOPE: Applied to ~90% ECR filtered dataset
        -- BUSINESS USE: Creates final linear intervals for strategic recommendations
        M AS optimal_bin_count
    FROM KNUTH_SCORES_2                 -- Source: Linear F(M) scores from step 5.8
    ORDER BY F_SCORE DESC              -- Sort by Bayesian evidence (highest first)
    LIMIT 1                            -- Select best M* for linear analysis
    -- RESULT: Optimal bin count for final linear-scale histogram
),
-- ===== STEP 5.10: FINAL LINEAR HISTOGRAM BIN AGGREGATION =====
-- PURPOSE: Re-aggregate linear micro-bins using optimal M* for final histogram
-- SCOPE: Create final linear-scale histogram on ~90% ECR filtered data
-- BUSINESS OUTPUT: Ready for second-level filtering and interval detection
FINAL_COUNTS_2 AS (
    SELECT 
        -- FINAL BIN INDEX: Map micro-bin u to final linear bin k using optimal M*
        -- FORMULA: k = floor(u * optimal_M / U) - same mapping as earlier stages
        -- RESULT: Linear bins with optimal granularity for strategic analysis
        CAST(FLOOR( (MC.u * B.optimal_bin_count) / P.U ) AS INT) AS k,
        
        -- FREQUENCY AGGREGATION: Total filtered orders in this final linear bin
        -- SCOPE: Orders from ~90% ECR subset
        -- BUSINESS USE: Order volume in each optimal linear payment range
        SUM(MC.c) AS freq,
        
        -- ECR AGGREGATION: Total processing costs in this final linear bin
        -- SCOPE: ECR from ~90% ECR subset
        -- STRATEGIC VALUE: Cost concentration in each optimal linear payment range
        SUM(MC.ecr_total) AS ecr_total
        
    FROM MICRO_COUNTS_2 MC              -- Source: Linear micro-bin data from step 5.5
    JOIN BEST_M_2 B ON TRUE             -- Cross join: Get optimal linear bin count
    JOIN PARAMS_2 P ON TRUE             -- Cross join: Get micro-bin count U
    GROUP BY CAST(FLOOR( (MC.u * B.optimal_bin_count) / P.U ) AS INT)
    -- RESULT: Final linear histogram bins ready for second-level filtering
),
-- ===== STEP 5.11: COMPLETE LINEAR BIN INDEX GENERATION =====
-- PURPOSE: Generate all bin indices k = 0, 1, 2, ..., M*-1 for linear histogram
-- METHODOLOGY: Same approach as Stage 2, applied to linear optimal bin count
-- COVERAGE: Ensures all linear bins are represented, including empty ones
BINS_ALL_2 AS (
    SELECT 
        -- LINEAR BIN INDEX: Sequential bin number from 0 to M*-1
        -- RANGE: k ∈ {0, 1, 2, ..., optimal_linear_bin_count-1}
        -- PURPOSE: Complete coverage for linear histogram construction
        ROW_NUMBER() OVER (ORDER BY seq4()) - 1 AS k
        
    FROM TABLE(GENERATOR(ROWCOUNT=>100000))   -- Generate large sequence (Snowflake function)
    JOIN BEST_M_2 B ON TRUE                   -- Cross join: Get optimal linear bin count
    QUALIFY k < B.optimal_bin_count           -- Filter: Keep only k < M*_linear
    -- RESULT: Complete sequence of linear bin indices
),
-- ===== STEP 5.12: LINEAR SCALE BIN EDGE CALCULATION =====
-- PURPOSE: Create equal-width bins in linear USD space using optimal M*
-- KEY DIFFERENCE: Direct USD division (no log transformation)
-- BUSINESS ADVANTAGE: Intuitive dollar amount intervals for strategic decisions
LINEAR_EDGES AS (
    SELECT 
        BA.k,                               -- Linear bin index (0 to M*-1)
        
        -- LEFT EDGE IN USD: y_left = Y_MIN + (range * k/M*)
        -- FORMULA: Linear interpolation in USD space (not log space)
        -- BUSINESS MEANING: Start of linear payment amount range for bin k
        S.Y_MIN + (S.Y_MAX - S.Y_MIN) * (BA.k / B.optimal_bin_count) AS y_left,
        
        -- RIGHT EDGE IN USD: y_right = Y_MIN + (range * (k+1)/M*)
        -- FORMULA: Linear interpolation for end of bin k
        -- BUSINESS MEANING: End of linear payment amount range for bin k
        S.Y_MIN + (S.Y_MAX - S.Y_MIN) * ((BA.k+1.0) / B.optimal_bin_count) AS y_right
        
    FROM BINS_ALL_2 BA                  -- Source: Complete linear bin indices from step 5.11
    JOIN BEST_M_2 B ON TRUE             -- Cross join: Get optimal linear bin count M*
    JOIN STATS_2 S ON TRUE              -- Cross join: Get linear USD bounds [Y_MIN, Y_MAX]
    -- RESULT: Linear USD boundaries for each bin k
),
-- ===== STEP 5.13: LINEAR USD BIN DEFINITION =====
-- PURPOSE: Define final USD bin boundaries for linear-scale analysis
-- NO TRANSFORMATION: Direct use of linear USD boundaries (unlike Stage 2's exp() transformation)
-- BOUNDARY PRECISION: Use exact data min/max for first and last bins
USD_BINS_2 AS (
    SELECT 
        L.k,                                -- Linear bin index (0 to M*-1)
        
        -- LEFT EDGE IN USD: Direct linear boundary (no exp transformation)
        -- SPECIAL CASE: First bin uses exact filtered data minimum
        -- PURPOSE: Precise boundary alignment with filtered dataset
        CASE WHEN L.k = 0 THEN 
            (SELECT MIN(PAYMENT_AMOUNT_USD) FROM FILTERED_APPROVED_ECR_TABLE WHERE PAYMENT_AMOUNT_USD > 0)
        ELSE L.y_left END AS edge_left_usd,
        
        -- RIGHT EDGE IN USD: Direct linear boundary
        -- SPECIAL CASE: Last bin uses exact filtered data maximum
        -- PURPOSE: Complete coverage of filtered payment amount range
        CASE WHEN L.k = B.optimal_bin_count - 1 THEN 
            (SELECT MAX(PAYMENT_AMOUNT_USD) FROM FILTERED_APPROVED_ECR_TABLE)
        ELSE L.y_right END AS edge_right_usd,
        
        -- BIN CENTER IN USD: Arithmetic mean of linear boundaries
        -- FORMULA: (left + right) / 2 - appropriate for linear scale
        -- BUSINESS USE: Representative payment amount for strategic analysis
        (L.y_left + L.y_right)/2.0 AS bin_center_usd
        
    FROM LINEAR_EDGES L                 -- Source: Linear USD boundaries from step 5.12
    JOIN BEST_M_2 B ON TRUE             -- Cross join: Get optimal bin count for boundary detection
    -- RESULT: Final linear USD bin definitions
),
-- ===== STEP 5.14: COMPLETE LINEAR HISTOGRAM CONSTRUCTION =====
-- PURPOSE: Combine linear bin definitions with actual frequencies and ECR totals
-- METHOD: LEFT JOIN to include all bins (even empty ones) with zero-filled missing data
-- SCOPE: Final linear-scale histogram on ~90% ECR filtered data
HIST_2 AS (
    SELECT
        U.k,                                -- Linear bin index (0 to M*_linear-1)
        
        -- LINEAR BIN BOUNDARIES: Direct USD payment amount ranges
        -- ADVANTAGE: Intuitive dollar intervals for business strategy
        U.edge_left_usd,                   -- Start of linear payment range
        U.edge_right_usd,                  -- End of linear payment range
        U.bin_center_usd,                  -- Representative amount for this range
        
        -- ORDER FREQUENCY: Number of filtered orders in this linear payment range
        -- ZERO-FILL: Empty bins get frequency = 0 (complete histogram coverage)
        COALESCE(F.freq, 0) AS freq,
        
        -- ECR CONCENTRATION: Total processing costs in this linear payment range
        -- ZERO-FILL: Empty bins get ecr_total = 0
        -- STRATEGIC VALUE: Direct cost concentration for business decisions
        COALESCE(F.ecr_total, 0) AS ecr_total
        
    FROM USD_BINS_2 U                   -- Source: Linear USD bin definitions from step 5.13
    LEFT JOIN FINAL_COUNTS_2 F USING (k) -- Left join: Include all bins, fill missing with NULL
    -- RESULT: Complete linear histogram ready for second-level filtering
),
-- =============================================================================
-- ===== STAGE 6: SECOND-LEVEL FILTERING BASED ON LINEAR ANALYSIS =====
-- =============================================================================
-- PURPOSE: Apply second Pareto-style filter to linear histogram results
-- STRATEGY: Rank linear bins by ECR and select top 90% for final interval detection
-- CUMULATIVE EFFECT: ~81% of original ECR (90% × 90%) in most cost-concentrated ranges

-- ===== STEP 6.1: LINEAR SCALE ECR RANKING AND CUMULATIVE ANALYSIS =====
-- PURPOSE: Rank linear histogram bins by ECR concentration for second-level filtering
-- SCOPE: Applied to ~90% ECR subset from Stage 4, now analyzed in linear scale
-- BUSINESS GOAL: Identify most cost-concentrated linear payment intervals
LINEAR_SECOND_FILTER AS (
    SELECT 
        k,                                  -- Linear bin index
        
        -- LINEAR BIN BOUNDARIES: Direct USD payment amount ranges
        -- STRATEGIC VALUE: Exact dollar intervals for business implementation
        edge_left_usd,                     -- Start of linear payment range
        edge_right_usd,                    -- End of linear payment range
        bin_center_usd,                    -- Representative payment amount
        
        -- BIN METRICS: Order volume and cost concentration
        freq,                               -- Number of orders in this linear range
        ecr_total,                         -- Total processing costs in this linear range
        
        -- CUMULATIVE ECR ANALYSIS: Running total for second-level filtering
        -- CALCULATION: Sum ECR from highest-cost linear bins to current bin
        -- PURPOSE: Identify 90% ECR threshold in linear scale
        SUM(ecr_total) OVER (ORDER BY ecr_total DESC) AS cumulative_ecr,
        
        -- CUMULATIVE ECR PERCENTAGE: Fraction of filtered ECR covered so far
        -- DENOMINATOR: Total ECR in filtered dataset (not original total)
        -- USAGE: Second-level 90% threshold calculation
        SUM(ecr_total) OVER (ORDER BY ecr_total DESC) / SUM(ecr_total) OVER () AS cumulative_ecr_pct,
        
        -- SECOND-LEVEL RANGE FLAG: Mark linear bins contributing to top 90% of filtered ECR
        -- LOGIC: 1 if this linear bin is part of top 90% ECR concentration, 0 otherwise
        -- FINAL FILTER: Will create ~81% ECR subset for interval detection
        CASE 
            WHEN SUM(ecr_total) OVER (ORDER BY ecr_total DESC) / SUM(ecr_total) OVER () < 0.9 THEN 1
            ELSE 0
        END AS in_range_of_interest
        
    FROM HIST_2                         -- Source: Complete linear histogram from step 5.14
    ORDER BY ecr_total DESC             -- Sort by linear ECR total (highest cost first)
    -- RESULT: Ranked linear bins with second-level filtering flags
),
-- =============================================================================
-- ===== STAGE 7: FINAL ORDER EXTRACTION BASED ON DUAL ECR FILTERING =====
-- =============================================================================
-- PURPOSE: Extract orders that pass both logarithmic and linear ECR concentration filters
-- METHODOLOGY: Two-stage filtering cascade - log-scale → linear-scale
-- BUSINESS OUTCOME: Core payment ranges with highest strategic cost optimization potential

-- ===== STEP 7.1: DUAL-FILTERED ORDER EXTRACTION =====
-- PURPOSE: Apply second-level filter to create final high-ECR order subset
-- FILTERING CASCADE: Stage 4 (90% ECR) → Stage 6 (90% of 90% ECR) = ~81% total ECR
-- STRATEGIC VALUE: Most cost-concentrated orders for targeted optimization
FINAL_APPROVED_ECR_TABLE AS (
    SELECT 
        -- PASS-THROUGH: All order attributes from first-level filtered dataset
        -- DUAL FILTERING: Orders that survive both log-scale and linear-scale ECR filtering
        -- BUSINESS RESULT: Highest strategic value transactions for cost optimization
        FILTERED_APPROVED_ECR_TABLE.*
        
    FROM FILTERED_APPROVED_ECR_TABLE    -- Source: Stage 4 filtered orders (~90% ECR)
    
    -- JOIN CONDITION: Match orders to their corresponding linear histogram bins
    -- RANGE CHECK: Order amount must fall within linear bin boundaries
    JOIN LINEAR_SECOND_FILTER ON FILTERED_APPROVED_ECR_TABLE.PAYMENT_AMOUNT_USD BETWEEN LINEAR_SECOND_FILTER.edge_left_usd AND LINEAR_SECOND_FILTER.edge_right_usd
    
    -- SECOND-LEVEL FILTER: Only include linear bins with high ECR concentration
    -- THRESHOLD: in_range_of_interest = 1 means top 90% ECR in linear analysis
    WHERE LINEAR_SECOND_FILTER.in_range_of_interest = 1
    
    -- RESULT: Final orders subset (~81% original ECR) for interval detection
),

-- =============================================================================
-- ===== STAGE 8: FINAL 1-USD HISTOGRAM FOR STRATEGIC INTERVAL DETECTION =====
-- =============================================================================
-- PURPOSE: Create high-resolution 1-USD histogram on dual-filtered data for precise intervals
-- SCOPE: Applied to ~81% ECR core subset from Stage 7 dual filtering
-- BUSINESS GOAL: Identify exact dollar amount ranges for strategic cost optimization

-- ===== STEP 8.1: 1-USD HISTOGRAM BOUNDARY CALCULATION =====
-- PURPOSE: Define precise bounds for 1-USD bin histogram
-- METHODOLOGY: Floor/ceil to ensure complete coverage of final filtered range
-- RESOLUTION: 1-USD bins for maximum strategic precision
PARAMS_3 AS (
    SELECT 
        -- HISTOGRAM MINIMUM: Floor of lowest payment amount in final dataset
        -- PURPOSE: Start 1-USD histogram at integer boundary below data minimum
        -- BUSINESS USE: Clean starting point for strategic interval definition
        FLOOR(MIN(PAYMENT_AMOUNT_USD)) AS bin_min,
        
        -- HISTOGRAM MAXIMUM: Ceiling of highest payment amount in final dataset
        -- PURPOSE: End 1-USD histogram at integer boundary above data maximum
        -- BUSINESS USE: Complete coverage of core ECR payment range
        CEIL(MAX(PAYMENT_AMOUNT_USD)) AS bin_max
        
    FROM FINAL_APPROVED_ECR_TABLE       -- Source: Dual-filtered orders (~81% ECR) from step 7.1
    -- RESULT: Integer bounds for 1-USD bin histogram construction
),
-- ===== STEP 8.2: 1-USD BIN SEQUENCE GENERATION =====
-- PURPOSE: Create comprehensive 1-USD bins covering entire core ECR range
-- RESOLUTION: Maximum precision with 1-dollar intervals for strategic decision-making
-- COVERAGE: From floor(min) to ceil(max) of dual-filtered payment amounts
BINS_3 AS (
    SELECT 
        -- BIN INDEX: Sequential numbering for 1-USD bins
        -- RANGE: k = 0, 1, 2, ..., (bin_max - bin_min)
        ROW_NUMBER() OVER (ORDER BY seq4()) - 1 AS k,
        
        -- LEFT EDGE: Start of 1-USD payment range
        -- FORMULA: bin_min + k (where k = 0, 1, 2, ...)
        -- EXAMPLE: If bin_min = 15, then edges are 15, 16, 17, ...
        P.bin_min + (ROW_NUMBER() OVER (ORDER BY seq4()) - 1) AS edge_left_usd,
        
        -- RIGHT EDGE: End of 1-USD payment range
        -- FORMULA: bin_min + k + 1 = left_edge + 1
        -- RESULT: Each bin spans exactly $1.00
        P.bin_min + (ROW_NUMBER() OVER (ORDER BY seq4())) AS edge_right_usd,
        
        -- BIN CENTER: Middle point of 1-USD range
        -- FORMULA: bin_min + k + 0.5 = left_edge + $0.50
        -- BUSINESS USE: Representative payment amount for this dollar range
        P.bin_min + (ROW_NUMBER() OVER (ORDER BY seq4()) - 0.5) AS bin_center_usd
        
    FROM TABLE(GENERATOR(ROWCOUNT=>100000)) t  -- Generate large sequence
    JOIN PARAMS_3 P                           -- Cross join: Get histogram bounds
    QUALIFY edge_left_usd < P.bin_max         -- Filter: Stop at bin_max
    -- RESULT: Complete sequence of 1-USD bins for high-resolution analysis
),
-- ===== STEP 8.3: 1-USD BIN AGGREGATION =====
-- PURPOSE: Count orders and sum ECR within each 1-USD payment amount bin
-- SCOPE: Applied to dual-filtered core dataset (~81% of original ECR)
-- PRECISION: 1-dollar resolution for exact strategic interval identification
FINAL_COUNTS_3 AS (
    SELECT 
        B.k,                                -- 1-USD bin index
        
        -- ORDER FREQUENCY: Number of core ECR orders in this 1-USD range
        -- SCOPE: Orders from dual-filtered dataset (~81% original ECR)
        -- BUSINESS USE: Volume of strategically important transactions per dollar
        COUNT(*) AS freq,
        
        -- ECR CONCENTRATION: Total processing costs in this 1-USD range
        -- STRATEGIC VALUE: Exact cost burden for each dollar amount interval
        -- OPTIMIZATION TARGET: Highest ECR concentration bins for cost reduction
        SUM(F.ORDER_PROCESSOR_FEE_USD) AS ecr_total
        
    FROM FINAL_APPROVED_ECR_TABLE F     -- Source: Dual-filtered core orders from step 7.1
    
    -- RANGE JOIN: Match orders to 1-USD bins based on payment amount
    -- CONDITION: amount >= left_edge AND amount < right_edge
    -- PRECISION: Each bin covers exactly $1.00 range
    JOIN BINS_3 B ON F.PAYMENT_AMOUNT_USD >= B.edge_left_usd AND F.PAYMENT_AMOUNT_USD < B.edge_right_usd
    
    GROUP BY B.k                        -- Aggregate by 1-USD bin
    -- RESULT: High-resolution ECR distribution for strategic analysis
),
-- ===== STEP 8.4: COMPLETE 1-USD HISTOGRAM CONSTRUCTION =====
-- PURPOSE: Combine 1-USD bin definitions with actual order frequencies and ECR totals
-- METHOD: LEFT JOIN to ensure complete coverage including empty dollar ranges
-- STRATEGIC OUTPUT: Highest resolution ECR distribution for interval detection
HIST_3 AS (
    SELECT
        B.k,                                -- 1-USD bin index
        
        -- 1-USD BIN BOUNDARIES: Exact dollar amount ranges
        -- PRECISION: Each bin represents exactly $1.00 payment range
        -- STRATEGIC USE: Precise intervals for business optimization decisions
        B.edge_left_usd,                   -- Start of dollar range (e.g., $15.00)
        B.edge_right_usd,                  -- End of dollar range (e.g., $16.00)
        B.bin_center_usd,                  -- Representative amount (e.g., $15.50)
        
        -- ORDER FREQUENCY: Count of core ECR orders in this exact dollar range
        -- ZERO-FILL: Empty dollar ranges get frequency = 0 for complete histogram
        -- BUSINESS INSIGHT: Transaction volume at precise payment amounts
        COALESCE(F.freq, 0) AS freq,
        
        -- ECR CONCENTRATION: Total processing costs for this exact dollar range
        -- ZERO-FILL: Empty dollar ranges get ecr_total = 0
        -- STRATEGIC TARGET: Precise cost hot spots for optimization
        COALESCE(F.ecr_total, 0) AS ecr_total
        
    FROM BINS_3 B                       -- Source: 1-USD bin definitions from step 8.2
    LEFT JOIN FINAL_COUNTS_3 F USING (k) -- Left join: Include all dollar ranges, fill missing with 0
    -- RESULT: Complete 1-USD resolution histogram for strategic interval detection
),

-- ===== STEP 8.5: INTERMEDIATE HISTOGRAM WITH CUMULATIVE METRICS =====
-- PURPOSE: Add cumulative statistics and flag bins with/without ECR for sequence detection
-- METHOD: Window functions to track progressive ECR bin accumulation
-- BUSINESS GOAL: Prepare for continuous interval detection algorithm
HIST_WITH_CUMULATIVE AS (
    SELECT 
        -- BUSINESS-FRIENDLY INDEX: Convert zero-based k to one-based bin numbering
        -- TRANSFORMATION: k=0 becomes bin_number=1, k=1 becomes bin_number=2, etc.
        -- STRATEGIC USE: Intuitive bin identification for business stakeholders
        (k + 1) AS bin_number,
        
        -- 1-USD BIN BOUNDARIES: Copy exact dollar ranges from HIST_3
        -- PRECISION: Each bin spans exactly $1.00 for maximum strategic resolution
        edge_left_usd AS bin_start,         -- Start of dollar range (e.g., $15.00)
        edge_right_usd AS bin_end,          -- End of dollar range (e.g., $16.00)
        bin_center_usd AS bin_center,       -- Representative amount (e.g., $15.50)
        
        -- BIN WIDTH VERIFICATION: Always 1.0 USD for validation
        -- CALCULATION: end - start = 1.0
        -- QUALITY CHECK: Ensure consistent bin sizing
        (edge_right_usd - edge_left_usd) AS bin_width,
        
        -- CORE METRICS: Copy frequency and ECR from 1-USD histogram
        freq AS frequency,                  -- Order count in this dollar range
        ecr_total,                         -- Total processing costs in this range
        
        -- ECR PRESENCE FLAGS: Binary indicators for sequence detection
        -- HAS_ECR: 1 if bin contains any ECR (ecr_total > 0), 0 if empty
        -- BUSINESS USE: Identify dollar ranges with cost concentration
        CASE WHEN ecr_total > 0 THEN 1 ELSE 0 END AS has_ecr,
        
        -- ABSENCE FLAG: Inverse of has_ecr for gap analysis
        -- NOT_HAS_ECR: 1 if bin is empty (ecr_total = 0), 0 if contains ECR
        CASE WHEN ecr_total > 0 THEN 0 ELSE 1 END AS not_has_ecr,
        
        -- CUMULATIVE ECR BIN COUNT: Running total of bins with ECR
        -- WINDOW: From start of histogram to current bin
        -- BUSINESS USE: Track progressive accumulation of cost-active dollar ranges
        SUM(CASE WHEN ecr_total > 0 THEN 1 ELSE 0 END) OVER (ORDER BY k) AS cumulative_has_ecr
        
    FROM HIST_3                         -- Source: Complete 1-USD histogram from step 8.4
    -- RESULT: Enhanced histogram with sequence detection flags and cumulative metrics
),

-- ===== STEP 8.6: SEQUENCE START DETECTION =====
-- PURPOSE: Identify the beginning of continuous ECR-containing dollar ranges
-- METHOD: LAG window function to detect transitions from empty to ECR-containing bins
-- STRATEGIC GOAL: Mark start points of cost concentration intervals for business analysis
SEQUENCE_START AS (
    SELECT
        -- CORE BIN ATTRIBUTES: Pass through all 1-USD bin metrics
        bin_number,                         -- One-based bin index for business reference
        bin_start,                         -- Start of $1.00 payment range
        bin_end,                           -- End of $1.00 payment range
        frequency,                         -- Order count in this dollar range
        ecr_total,                         -- Total processing costs in this range
        has_ecr,                           -- Binary flag: 1 if ECR > 0, 0 if empty
        
        -- SEQUENCE START FLAG: Mark beginning of continuous ECR intervals
        -- LOGIC: current bin has ECR AND previous bin had no ECR (or is first bin)
        -- TRANSITION: 0 → 1 in has_ecr sequence indicates interval start
        -- BUSINESS USE: Identify where cost concentration intervals begin
        CASE
            WHEN has_ecr = 1 AND COALESCE(LAG(has_ecr) OVER (ORDER BY bin_number), 0) = 0 THEN 1
            ELSE 0
        END AS start_flag
        
    FROM HIST_WITH_CUMULATIVE           -- Source: Enhanced histogram from step 8.5
    -- RESULT: Bins with sequence start detection for continuous interval grouping
),

-- ===== STEP 8.7: CONTINUOUS SEQUENCE GROUPING =====
-- PURPOSE: Group consecutive ECR-containing bins into continuous intervals
-- METHOD: Running sum of start_flags to assign unique group IDs to each continuous sequence
-- STRATEGIC OUTCOME: Identify unified payment amount ranges with sustained ECR concentration
GROUPED_SEQUENCES AS (
    SELECT
        -- CORE BIN ATTRIBUTES: Pass through all 1-USD bin metrics and flags
        bin_number,                         -- One-based bin index
        bin_start,                         -- Start of $1.00 payment range
        bin_end,                           -- End of $1.00 payment range
        frequency,                         -- Order count in this dollar range
        ecr_total,                         -- Total processing costs in this range
        has_ecr,                           -- Binary flag: 1 if ECR > 0, 0 if empty
        
        -- SEQUENCE GROUP ID: Assign unique identifier to each continuous ECR interval
        -- LOGIC: Running sum of start_flags creates incremental group numbering
        -- EXAMPLE: start_flags [1,0,0,1,0,1] → sequence_groups [1,1,1,2,2,3]
        -- BUSINESS USE: Group adjacent dollar ranges with sustained cost concentration
        SUM(start_flag) OVER (ORDER BY bin_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sequence_group
        
    FROM SEQUENCE_START                 -- Source: Bins with start detection from step 8.6
    -- RESULT: Bins with continuous sequence group assignments for aggregation
),

-- ===== STEP 8.8: CONTINUOUS INTERVAL AGGREGATION =====
-- PURPOSE: Collapse grouped sequences into unified payment amount intervals
-- METHOD: Aggregate consecutive 1-USD bins with sustained ECR concentration
-- STRATEGIC OUTCOME: Identify cohesive payment ranges for targeted cost optimization
CONTINUOUS_RUNS AS (
    SELECT
        sequence_group,                     -- Unique identifier for each continuous ECR interval
        
        -- INTERVAL BOUNDARIES: First and last bins in continuous sequence
        -- BIN RANGE: Identifies span of consecutive 1-USD bins with ECR
        -- BUSINESS USE: Define precise start/end of cost concentration intervals
        MIN(bin_number) AS first_bin,       -- First 1-USD bin in sequence
        MAX(bin_number) AS last_bin,        -- Last 1-USD bin in sequence
        
        -- PAYMENT AMOUNT BOUNDARIES: Exact dollar ranges for strategic intervals
        -- PRECISION: Start of first bin to end of last bin in continuous sequence
        -- STRATEGIC VALUE: Actionable payment ranges for business optimization
        MIN(bin_start) AS start_amount,     -- Start of continuous payment range
        MAX(bin_end) AS end_amount,         -- End of continuous payment range
        
        -- INTERVAL CHARACTERISTICS: Length and volume metrics
        -- RUN_LENGTH: Number of consecutive 1-USD bins in this interval
        -- EXAMPLE: $15-$19 range = 4 consecutive bins = run_length of 4
        COUNT(*) AS run_length,
        
        -- TOTAL FREQUENCY: Sum of order counts across all bins in interval
        -- SCOPE: Combined volume from all 1-USD bins in continuous range
        -- BUSINESS INSIGHT: Total transaction volume for this strategic interval
        SUM(frequency) AS total_frequency,
        
        -- TOTAL ECR: Sum of processing costs across all bins in interval
        -- STRATEGIC PRIORITY: Total cost burden for this payment amount range
        -- OPTIMIZATION TARGET: Intervals with highest total_ecr for cost reduction
        SUM(ecr_total) AS total_ecr
        
    FROM GROUPED_SEQUENCES              -- Source: Bins with sequence groups from step 8.7
    WHERE has_ecr = 1                   -- Filter: Only include bins with ECR concentration
    GROUP BY sequence_group             -- Aggregate: Collapse each sequence into single interval
    -- RESULT: Unified continuous intervals with total ECR and frequency metrics
),

-- ===== STEP 8.9: PRIMARY STRATEGIC INTERVAL SELECTION =====
-- PURPOSE: Select the single continuous interval with highest total ECR concentration
-- METHOD: Qualify by maximum total_ecr to identify the most cost-critical payment range
-- STRATEGIC OUTCOME: Primary target interval for cost optimization initiatives
FINAL_RESULT AS (
    SELECT
        *,                                  -- All continuous interval attributes from step 8.8
        
        -- INTERVAL WIDTH: Physical span of primary strategic interval
        -- CALCULATION: end_amount - start_amount (in USD)
        -- BUSINESS INSIGHT: Size of payment range requiring optimization focus
        end_amount - start_amount AS interval_width,
        
        -- ECR DENSITY: Cost concentration per dollar within this interval
        -- FORMULA: total_ecr ÷ interval_width = cost per USD of payment range
        -- STRATEGIC METRIC: Intensity of cost burden for prioritization decisions
        total_ecr / (end_amount - start_amount) AS ecr_density,
        
        -- INTERVAL CLASSIFICATION: Mark as primary large strategic interval
        -- BUSINESS TYPE: LARGE_INTEREST_INTERVAL = main cost optimization target
        'LARGE_INTEREST_INTERVAL' AS interval_type
        
    FROM CONTINUOUS_RUNS                -- Source: All continuous intervals from step 8.8
    
    -- PRIMARY SELECTION: Choose interval with maximum total ECR concentration
    -- STRATEGIC LOGIC: Highest cost burden = highest optimization priority
    QUALIFY total_ecr = MAX(total_ecr) OVER ()
    -- RESULT: Single primary interval with highest strategic value for cost reduction
),

-- ===== STEP 8.10: TOP INDIVIDUAL BIN IDENTIFICATION =====
-- PURPOSE: Identify the 3 individual 1-USD bins with highest ECR concentration
-- SCOPE: Complement to continuous intervals - highlights specific dollar hotspots
-- STRATEGIC VALUE: Precise single-dollar targets for immediate optimization
TOP_BINS AS (
    SELECT
        -- 1-USD BIN BOUNDARIES: Exact dollar ranges for top individual bins
        -- PRECISION: Each represents exactly $1.00 payment range with highest costs
        -- STRATEGIC TARGET: Specific dollar amounts for immediate cost reduction
        edge_left_usd AS start_amount,      -- Start of high-cost dollar range
        edge_right_usd AS end_amount,       -- End of high-cost dollar range
        
        -- BIN METRICS: Volume and cost concentration for this specific dollar
        -- FREQUENCY: Order count in this exact $1.00 range
        -- ECR_TOTAL: Total processing costs for this precise payment amount
        freq AS bin_frequency,              -- Transaction volume
        ecr_total AS bin_ecr_total,         -- Cost concentration
        
        -- INTERVAL CLASSIFICATION: Rank-based labeling for top cost hotspots
        -- RANKING: 1st, 2nd, 3rd highest ECR bins for targeted optimization
        -- BUSINESS TYPE: SHORT_INTEREST_INTERVAL = individual dollar hotspots
        CASE 
            WHEN ROW_NUMBER() OVER (ORDER BY ecr_total DESC) = 1 THEN 'SHORT_INTEREST_INTERVAL_1'
            WHEN ROW_NUMBER() OVER (ORDER BY ecr_total DESC) = 2 THEN 'SHORT_INTEREST_INTERVAL_2'
            WHEN ROW_NUMBER() OVER (ORDER BY ecr_total DESC) = 3 THEN 'SHORT_INTEREST_INTERVAL_3'
        END AS interval_type
        
    FROM HIST_3                         -- Source: Complete 1-USD histogram from step 8.4
    
    -- TOP 3 SELECTION: Limit to 3 highest ECR individual bins
    -- STRATEGIC FOCUS: Most cost-critical individual dollar ranges
    QUALIFY ROW_NUMBER() OVER (ORDER BY ecr_total DESC) <= 3
    -- RESULT: Top 3 individual $1.00 bins for immediate strategic attention
),

-- ===== STEP 8.11: STRATEGIC INTERVAL BOUNDARY EXTRACTION =====
-- PURPOSE: Extract bounds of primary strategic interval for context analysis
-- METHOD: Get min/max from FINAL_RESULT to define surrounding payment ranges
-- BUSINESS GOAL: Create complementary PRE and POST intervals for complete market coverage
GRANDE_INTERVAL_BOUNDS AS (
    SELECT 
        -- PRIMARY INTERVAL START: Lower bound of main cost optimization target
        -- SOURCE: Minimum start_amount from highest ECR continuous interval
        -- BUSINESS USE: Defines where strategic focus begins in payment spectrum
        MIN(start_amount) AS grande_start,
        
        -- PRIMARY INTERVAL END: Upper bound of main cost optimization target
        -- SOURCE: Maximum end_amount from highest ECR continuous interval
        -- BUSINESS USE: Defines where strategic focus ends in payment spectrum
        MAX(end_amount) AS grande_end,
        
        -- MARKET MAXIMUM: Absolute upper bound of all approved order amounts
        -- CALCULATION: Max payment amount + 1 for complete coverage
        -- BUSINESS USE: Define upper bound for POST-interval analysis
        (SELECT MAX(PAYMENT_AMOUNT_USD) + 1 FROM APPROVED_ECR_TABLE) AS max_amount
        
    FROM FINAL_RESULT                   -- Source: Primary strategic interval from step 8.9
    -- RESULT: Boundary definitions for complementary interval analysis
),

-- ===== STEP 8.12: COMPLEMENTARY INTERVAL CREATION =====
-- PURPOSE: Create PRE and POST intervals surrounding the primary strategic interval
-- METHOD: Use original APPROVED_ECR_TABLE to capture complete market context
-- BUSINESS GOAL: Provide full payment spectrum analysis for comprehensive strategy
PRE_POST_INTERVALS AS (
    -- ===== PRE-INTERVAL: Below Primary Strategic Range =====
    -- PURPOSE: Analyze payment amounts below the main cost optimization target
    -- SCOPE: From $0 to start of primary strategic interval
    SELECT
        -- PRE-INTERVAL BOUNDARIES: From market minimum to strategic interval start
        -- LOWER BOUND: $0 (market floor for comprehensive coverage)
        -- UPPER BOUND: grande_start (where primary strategic interval begins)
        0 AS start_amount,                  -- Market minimum: $0
        grande_start AS end_amount,         -- Primary interval start
        
        -- INTERVAL CLASSIFICATION: Mark as pre-strategic context
        -- BUSINESS TYPE: PRE_LARGE_INTERVAL = payment amounts below optimization focus
        'PRE_LARGE_INTERVAL' AS interval_type,
        
        -- PRE-INTERVAL METRICS: Volume and cost analysis below strategic range
        -- SOURCE: Original approved order dataset (not filtered)
        -- BUSINESS INSIGHT: Market context outside primary optimization target
        (SELECT COUNT(*) FROM APPROVED_ECR_TABLE WHERE PAYMENT_AMOUNT_USD BETWEEN 0 AND grande_start) AS frequency,
        (SELECT SUM(ORDER_PROCESSOR_FEE_USD) FROM APPROVED_ECR_TABLE WHERE PAYMENT_AMOUNT_USD BETWEEN 0 AND grande_start) AS ecr_total
        
    FROM GRANDE_INTERVAL_BOUNDS         -- Source: Primary interval bounds from step 8.11
    
    UNION ALL
    
    -- ===== POST-INTERVAL: Above Primary Strategic Range =====
    -- PURPOSE: Analyze payment amounts above the main cost optimization target
    -- SCOPE: From end of primary strategic interval to market maximum
    SELECT
        -- POST-INTERVAL BOUNDARIES: From strategic interval end to market maximum
        -- LOWER BOUND: grande_end (where primary strategic interval ends)
        -- UPPER BOUND: max_amount (market ceiling for comprehensive coverage)
        grande_end AS start_amount,         -- Primary interval end
        max_amount AS end_amount,           -- Market maximum: max payment + 1
        
        -- INTERVAL CLASSIFICATION: Mark as post-strategic context
        -- BUSINESS TYPE: POST_LARGE_INTERVAL = payment amounts above optimization focus
        'POST_LARGE_INTERVAL' AS interval_type,
        
        -- POST-INTERVAL METRICS: Volume and cost analysis above strategic range
        -- SOURCE: Original approved order dataset (not filtered)
        -- BUSINESS INSIGHT: Market context outside primary optimization target
        (SELECT COUNT(*) FROM APPROVED_ECR_TABLE WHERE PAYMENT_AMOUNT_USD BETWEEN grande_end AND max_amount) AS frequency,
        (SELECT SUM(ORDER_PROCESSOR_FEE_USD) FROM APPROVED_ECR_TABLE WHERE PAYMENT_AMOUNT_USD BETWEEN grande_end AND max_amount) AS ecr_total
        
    FROM GRANDE_INTERVAL_BOUNDS         -- Source: Primary interval bounds from step 8.11
    -- RESULT: Complete market context with PRE, PRIMARY, and POST payment intervals
),

-- ===== STEP 8.13: PRIMARY INTERVAL DETAILED BREAKDOWN =====
-- PURPOSE: Create high-resolution 1-USD histogram within primary strategic interval
-- METHOD: Generate precise bins from original data within FINAL_RESULT boundaries
-- STRATEGIC VALUE: Detailed cost distribution analysis within main optimization target
LARGE_INTEREST_INTERVAL_TABLE AS (
    SELECT
        -- BIN INDEX: Zero-based indexing for primary interval bins
        -- RANGE: k = 0, 1, 2, ..., width-1 where width = ceil(end - start)
        -- BUSINESS USE: Sequential numbering within strategic payment range
        ROW_NUMBER() OVER (ORDER BY seq) - 1 AS k,
        
        -- DYNAMIC BIN START: Precise left edge for each 1-USD bin within primary interval
        -- FIRST BIN: Starts exactly at primary interval start_amount
        -- SUBSEQUENT BINS: start_amount + (seq - 1) for exact 1-USD increments
        -- STRATEGIC PRECISION: Exact dollar boundaries within optimization target
        CASE WHEN seq = 1 THEN start_amount ELSE start_amount + seq - 1 END AS bin_start,
        
        -- DYNAMIC BIN END: Precise right edge for each 1-USD bin within primary interval
        -- LAST BIN: Ends exactly at primary interval end_amount
        -- INTERMEDIATE BINS: start_amount + seq for exact 1-USD increments
        -- STRATEGIC PRECISION: Complete coverage of optimization target range
        CASE WHEN seq = CEIL(end_amount - start_amount) THEN end_amount ELSE start_amount + seq END AS bin_end,
        
        -- BIN FREQUENCY: Count orders in each 1-USD range within primary interval
        -- SOURCE: Original APPROVED_ECR_TABLE (not filtered) for complete accuracy
        -- RANGE CHECK: amount >= bin_start AND amount < bin_end
        -- BUSINESS INSIGHT: Transaction volume at each dollar within strategic range
        (SELECT COUNT(*) 
         FROM APPROVED_ECR_TABLE 
         WHERE PAYMENT_AMOUNT_USD >= 
            (CASE WHEN seq = 1 THEN start_amount ELSE start_amount + seq - 1 END)
         AND PAYMENT_AMOUNT_USD < 
            (CASE WHEN seq = CEIL(end_amount - start_amount) THEN end_amount ELSE start_amount + seq END)
        ) AS frequency,
        
        -- BIN ECR TOTAL: Sum processing costs in each 1-USD range within primary interval
        -- SOURCE: Original APPROVED_ECR_TABLE for complete cost calculation
        -- STRATEGIC TARGET: Exact cost burden per dollar within optimization focus
        (SELECT SUM(ORDER_PROCESSOR_FEE_USD) 
         FROM APPROVED_ECR_TABLE 
         WHERE PAYMENT_AMOUNT_USD >= 
            (CASE WHEN seq = 1 THEN start_amount ELSE start_amount + seq - 1 END)
         AND PAYMENT_AMOUNT_USD < 
            (CASE WHEN seq = CEIL(end_amount - start_amount) THEN end_amount ELSE start_amount + seq END)
        ) AS ecr_total
        
    FROM (
        -- SEQUENCE GENERATION: Create bins covering primary strategic interval
        -- CROSS JOIN: Combine interval bounds with sequential numbering
        -- BIN COUNT: ceil(end_amount - start_amount) = number of 1-USD bins needed
        SELECT F.start_amount, F.end_amount, seq4() AS seq
        FROM FINAL_RESULT F                 -- Source: Primary strategic interval
        CROSS JOIN TABLE(GENERATOR(ROWCOUNT=>100000))  -- Generate sequence numbers
        WHERE F.interval_type = 'LARGE_INTEREST_INTERVAL'  -- Filter: Only primary interval
        QUALIFY seq <= CEIL(F.end_amount - F.start_amount)  -- Limit: Exact bin count needed
    )
    -- RESULT: High-resolution breakdown of primary strategic interval for detailed analysis
),
-- ===== STEP 8.14: TOTAL MARKET BASELINE CALCULATION =====
-- PURPOSE: Calculate total approved order volume and ECR for percentage computations
-- SCOPE: Complete approved order market for baseline comparison
-- STRATEGIC USE: Denominator for calculating market share and ECR concentration percentages
TOTAL_APPROVED AS (
    SELECT 
        -- TOTAL ORDER VOLUME: Complete count of approved orders in market
        -- SCOPE: All approved orders regardless of payment amount
        -- BUSINESS USE: Baseline for calculating percentage market coverage
        COUNT(*) AS total_approved_orders,
        
        -- TOTAL ECR BURDEN: Complete processing cost burden across market
        -- SCOPE: Sum of all processing fees from approved orders
        -- STRATEGIC BASELINE: Total cost pool for optimization percentage calculations
        SUM(ORDER_PROCESSOR_FEE_USD) AS total_ecr
        
    FROM APPROVED_ECR_TABLE             -- Source: Complete approved order dataset
    -- RESULT: Market totals for strategic interval percentage analysis
),
-- ===== STEP 8.15: PRIMARY INTERVAL BASELINE CALCULATION =====
-- PURPOSE: Calculate totals specifically for primary strategic interval for relative percentages
-- SCOPE: Orders within FINAL_RESULT boundaries only
-- STRATEGIC USE: Denominator for internal interval percentage analysis
TOTAL_APPROVED_LARGE_INTEREST_INTERVAL AS (
    SELECT 
        -- PRIMARY INTERVAL ORDER VOLUME: Count of orders within strategic range
        -- SCOPE: Only orders between start_amount and end_amount of primary interval
        -- BUSINESS USE: Baseline for calculating intra-interval percentage distributions
        COUNT(*) AS total_approved_orders,
        
        -- PRIMARY INTERVAL ECR BURDEN: Processing costs within strategic range
        -- SCOPE: Sum of fees from orders in primary optimization target
        -- STRATEGIC USE: Baseline for internal ECR concentration analysis
        SUM(ORDER_PROCESSOR_FEE_USD) AS total_ecr
        
    FROM APPROVED_ECR_TABLE             -- Source: Complete approved order dataset
    
    -- BOUNDARY FILTER: Only include orders within primary strategic interval
    -- RANGE: Between start_amount and end_amount from FINAL_RESULT
    -- PURPOSE: Isolate primary interval for internal percentage calculations
    WHERE PAYMENT_AMOUNT_USD BETWEEN (SELECT start_amount FROM FINAL_RESULT WHERE interval_type = 'LARGE_INTEREST_INTERVAL') AND (SELECT end_amount FROM FINAL_RESULT WHERE interval_type = 'LARGE_INTEREST_INTERVAL')
    -- RESULT: Primary interval totals for relative percentage analysis
),
-- ===== STEP 8.16: COMPREHENSIVE PERCENTAGE ANALYSIS =====
-- PURPOSE: Calculate cumulative percentages and ECR density metrics for primary interval bins
-- METHOD: Window functions for running totals with multiple baseline comparisons
-- STRATEGIC VALUE: Progressive analysis of cost concentration within optimization target
PERCENTAGE_ECR AS (
    SELECT
        -- CORE BIN ATTRIBUTES: Pass through primary interval bin characteristics
        k,                                  -- Zero-based bin index within primary interval
        bin_start,                         -- Start of 1-USD range within strategic interval
        bin_end,                           -- End of 1-USD range within strategic interval
        frequency,                         -- Order count in this specific dollar range
        ecr_total,                         -- Processing cost total in this dollar range
        
        -- CUMULATIVE FREQUENCY: Running total of orders within primary interval
        -- EXCLUSION: k=0 bin excluded from cumulative analysis
        -- BUSINESS INSIGHT: Progressive order volume accumulation
        SUM(CASE WHEN k > 0 THEN frequency ELSE 0 END) OVER (ORDER BY k) AS cumulative_frequency,
        
        -- CUMULATIVE ECR: Running total of processing costs within primary interval
        -- EXCLUSION: k=0 bin excluded from cumulative analysis
        -- STRATEGIC METRIC: Progressive cost burden accumulation
        SUM(CASE WHEN k > 0 THEN ecr_total ELSE 0 END) OVER (ORDER BY k) AS cumulative_ecr,
        
        -- INTRA-INTERVAL ORDER PERCENTAGE: Cumulative % relative to primary interval total
        -- DENOMINATOR: Total orders within strategic interval only
        -- BUSINESS USE: Distribution analysis within optimization target
        CAST((SUM(CASE WHEN k > 0 THEN frequency ELSE 0 END) OVER (ORDER BY k) / 
             (SELECT SUM(frequency) FROM LARGE_INTEREST_INTERVAL_TABLE WHERE k > 0)) * 100 AS DECIMAL(10,2)) AS cumulative_percentage_relative_to_total_orders_of_interest,
        
        -- INTRA-INTERVAL ECR PERCENTAGE: Cumulative % relative to primary interval ECR
        -- DENOMINATOR: Total ECR within strategic interval only
        -- STRATEGIC METRIC: Internal cost concentration distribution
        CAST((SUM(CASE WHEN k > 0 THEN ecr_total ELSE 0 END) OVER (ORDER BY k) / 
             (SELECT SUM(ecr_total) FROM LARGE_INTEREST_INTERVAL_TABLE WHERE k > 0)) * 100 AS DECIMAL(10,2)) AS cumulative_percentage_relative_to_total_ecr_of_interest,
        
        -- MARKET-WIDE ORDER PERCENTAGE: Cumulative % relative to entire approved order market
        -- DENOMINATOR: Total approved orders across all payment amounts
        -- BUSINESS INSIGHT: Market share captured by strategic interval progression
        CAST((SUM(CASE WHEN k > 0 THEN frequency ELSE 0 END) OVER (ORDER BY k) / (SELECT total_approved_orders FROM TOTAL_APPROVED)) * 100 AS DECIMAL(10,2)) AS cumulative_percentage_relative_to_total_approved_orders,
        
        -- MARKET-WIDE ECR PERCENTAGE: Cumulative % relative to entire market ECR burden
        -- DENOMINATOR: Total ECR across all approved orders
        -- STRATEGIC PRIORITY: Market cost impact of optimization target progression
        CAST((SUM(CASE WHEN k > 0 THEN ecr_total ELSE 0 END) OVER (ORDER BY k) / (SELECT total_ecr FROM TOTAL_APPROVED)) * 100 AS DECIMAL(10,2)) AS cumulative_percentage_relative_to_total_ecr,
        
        -- ECR DENSITY: Cost concentration per dollar within each 1-USD bin
        -- CALCULATION: ecr_total ÷ bin_width (always 1.0 for 1-USD bins)
        -- STRATEGIC METRIC: Cost intensity for prioritization within interval
        CAST((ecr_total / (bin_end - bin_start)) AS DECIMAL(10,4)) AS ecr_density
        
    FROM LARGE_INTEREST_INTERVAL_TABLE  -- Source: Primary interval breakdown from step 8.13
    -- RESULT: Comprehensive percentage analysis for strategic decision-making
),
-- ===== STEP 8.17: FILTERED PERCENTAGE ANALYSIS =====
-- PURPOSE: Remove k=0 bin and create clean ordered dataset for summary analysis
-- METHOD: Filter out zero-index bin and ensure proper ordering by bin sequence
-- STRATEGIC USE: Clean dataset for interval threshold calculations and summaries
PERCENTAGE_ECR_2 AS (
    -- FILTER AND ORDER: Remove k=0 bin (often empty/boundary) and sort by bin index
    -- EXCLUSION: k=0 represents boundary bin that may skew percentage analysis
    -- ORDERING: Ensure sequential progression for cumulative percentage thresholds
    -- BUSINESS OUTPUT: Clean progression of cost concentration for decision-making
    SELECT * FROM PERCENTAGE_ECR        -- Source: Comprehensive percentage analysis from step 8.16
    WHERE k > 0                         -- Filter: Exclude boundary bin
    ORDER BY k                          -- Order: Sequential bin progression
    -- RESULT: Clean ordered dataset for strategic interval threshold analysis
),

-- ===== STEP 8.18: STRATEGIC INTERVAL CLASSIFICATION SUMMARY =====
-- PURPOSE: Create three-tiered strategic interval classification for business decision-making
-- METHOD: Define LARGE (complete), MEDIUM (80% threshold), SMALL (peak bin) interval types
-- BUSINESS VALUE: Multiple strategic options from comprehensive to focused optimization targets
SUMMARY_TABLE AS (
    -- ===== LARGE INTERVAL: COMPLETE PRIMARY STRATEGIC RANGE =====
    -- PURPOSE: Full span of primary strategic interval for comprehensive optimization
    -- SCOPE: From minimum to maximum bin boundaries within strategic range
    -- BUSINESS USE: Complete cost optimization target for full strategic initiative
    SELECT
        -- LARGE INTERVAL BOUNDARIES: Complete span of primary strategic interval
        -- START: Minimum bin_start from filtered percentage analysis
        -- END: Maximum bin_end from filtered percentage analysis
        -- STRATEGIC SCOPE: Full payment range identified by dual ECR filtering
        (SELECT MIN(bin_start) FROM PERCENTAGE_ECR_2) AS init,
        (SELECT MAX(bin_end) FROM PERCENTAGE_ECR_2) AS end,
        
        -- LARGE INTERVAL METRICS: Complete volume and cost totals
        -- FREQUENCY: Sum of all orders within primary strategic interval
        -- ECR_TOTAL: Sum of all processing costs within strategic range
        -- BUSINESS IMPACT: Total optimization potential for comprehensive initiative
        (SELECT SUM(frequency) FROM PERCENTAGE_ECR_2) AS frequency,
        (SELECT SUM(ecr_total) FROM PERCENTAGE_ECR_2) AS total_ecr,
        
        -- INTERVAL CLASSIFICATION: Mark as complete strategic range
        -- TYPE: LARGE = comprehensive optimization target
        'LARGE' AS interval_type
    
    
    UNION ALL
    
    -- ===== SMALL INTERVAL: PEAK ECR CONCENTRATION BIN =====
    -- PURPOSE: Single 1-USD bin with highest absolute ECR concentration
    -- METHOD: Select individual bin with maximum ecr_total value
    -- BUSINESS USE: Immediate quick-win optimization target for rapid results
    SELECT
        -- SMALL INTERVAL BOUNDARIES: Single 1-USD bin with peak cost concentration
        -- PRECISION: Exact $1.00 payment range with highest processing costs
        -- STRATEGIC FOCUS: Immediate optimization opportunity for quick impact
        P.bin_start AS init,           -- Start of peak cost dollar range
        P.bin_end AS end,             -- End of peak cost dollar range
        
        -- SMALL INTERVAL METRICS: Volume and cost for peak concentration bin
        -- FREQUENCY: Order count in highest cost dollar range
        -- ECR_TOTAL: Maximum processing cost concentration in single dollar
        -- BUSINESS IMPACT: Highest cost density for immediate optimization
        P.frequency AS frequency,      -- Transaction volume in peak bin
        P.ecr_total AS total_ecr,     -- Peak cost concentration
        
        -- INTERVAL CLASSIFICATION: Mark as immediate optimization target
        -- TYPE: SMALL = single-dollar peak for quick-win initiatives
        'SMALL' AS interval_type
        
    FROM (
        -- PEAK BIN SELECTION: Identify single bin with highest ECR total
        -- RANKING: Order by ecr_total DESC to find maximum cost concentration
        -- LIMIT: Select only the top 1 bin for immediate optimization focus
        SELECT bin_start, bin_end, frequency, ecr_total
        FROM PERCENTAGE_ECR_2          -- Source: Clean percentage analysis from step 8.17
        ORDER BY ecr_total DESC        -- Sort: Highest ECR concentration first
        LIMIT 1                        -- Select: Peak cost concentration bin only
    ) P
    -- RESULT: Three-tiered strategic interval classification for flexible optimization approaches
)

-- =============================================================================
-- ===== FINAL OUTPUT: STRATEGIC ECR INTERVAL ANALYSIS RESULTS =====
-- =============================================================================
-- PURPOSE: Present three-tiered strategic interval classification with comprehensive metrics
-- BUSINESS VALUE: Multiple optimization approaches from comprehensive to focused quick-wins
-- STRATEGIC OUTCOME: Actionable payment amount ranges with cost optimization potential

SELECT 
    -- PRIMARY CLASSIFICATION: Three-tiered strategic approach
    -- LARGE: Complete primary interval for comprehensive optimization
    -- MEDIUM: 80% ECR threshold subset for focused high-impact optimization
    -- SMALL: Peak ECR bin for immediate quick-win optimization
    interval_type,
    
    -- INTERVAL BOUNDARIES: Exact USD payment amount ranges for business implementation
    -- STRATEGIC USE: Define precise payment amounts for targeted optimization initiatives
    -- BUSINESS PRECISION: Dollar-exact boundaries for operational decision-making
    init as USD_BIN_INIT,           -- Starting USD amount of strategic interval
    end as USD_BIN_END,             -- Ending USD amount of strategic interval
    
    -- INTERVAL CHARACTERISTICS: Physical span metrics for strategic planning
    -- WIDTH CALCULATION: end - init = total USD span of optimization target
    -- BUSINESS INSIGHT: Size of payment range requiring optimization resources
    end - init AS bin_width,        -- Width of strategic interval in USD
    
    -- VOLUME METRICS: Transaction count and processing cost totals
    -- FREQUENCY: Number of approved orders within this strategic interval
    -- ECR_TOTAL: Sum of processing fees (ORDER_PROCESSOR_FEE_USD) in interval
    -- STRATEGIC PRIORITY: Higher totals indicate greater optimization potential
    frequency,                      -- Number of approved orders in interval
    ROUND(total_ecr, 0) AS total_ecr, -- Total ECR processing costs in interval (rounded)
    
    -- ECR DENSITY: Cost concentration per dollar of payment range
    -- CALCULATION: total_ecr ÷ interval_width = cost intensity
    -- STRATEGIC METRIC: Higher density indicates more concentrated optimization opportunity
    -- BUSINESS USE: Prioritize intervals with highest cost concentration per dollar
    ROUND((total_ecr / (end - init)), 0) AS ecr_density,
    
    -- NORMALIZED ECR EFFICIENCY: ECR percentage adjusted for interval width
    -- FORMULA: (interval_ecr ÷ market_ecr) ÷ interval_width × 100
    -- STRATEGIC INSIGHT: Cost efficiency accounting for range size
    -- BUSINESS VALUE: Compare optimization efficiency across different interval sizes
    ROUND((total_ecr / ((SELECT total_ecr FROM TOTAL_APPROVED)*(end - init))) * 100, 2) AS percentage_of_total_ecr_divided_by_width,
    
    -- MARKET ECR SHARE: Percentage of total market ECR captured by this interval
    -- CALCULATION: (interval_ecr ÷ total_market_ecr) × 100
    -- STRATEGIC SIGNIFICANCE: Market cost impact of optimizing this interval
    -- BUSINESS PRIORITY: Higher percentages represent greater market cost reduction potential
    ROUND((total_ecr / (SELECT total_ecr FROM TOTAL_APPROVED)) * 100, 2) AS percentage_of_total_ecr,
    
    -- MARKET VOLUME SHARE: Percentage of total approved orders in this interval
    -- CALCULATION: (interval_frequency ÷ total_approved_orders) × 100
    -- BUSINESS INSIGHT: Market transaction coverage of optimization target
    -- STRATEGIC CONTEXT: Volume impact alongside cost concentration
    ROUND((frequency / (SELECT total_approved_orders FROM TOTAL_APPROVED)) * 100, 2) AS percentage_of_approved_orders,
    
    -- PROCESSOR FEE USD SHARE: Percentage of total processor fees in USD this interval represents
    -- CALCULATION: (interval_total_ecr ÷ total_market_processor_fees_usd) × 100
    -- BUSINESS VALUE: Direct financial impact of this interval on total processing costs
    -- STRATEGIC PRIORITY: Higher percentages indicate greater fee reduction opportunities
    ROUND((total_ecr / (SELECT total_ecr FROM TOTAL_APPROVED)) * 100, 2) AS percentage_of_total_processor_fees_usd,
    
    -- ORDER DENSITY: Transaction concentration per dollar of payment range
    -- CALCULATION: frequency ÷ interval_width = orders per USD of range
    -- OPERATIONAL INSIGHT: Transaction volume intensity within strategic interval
    -- BUSINESS USE: Understand order concentration for operational planning
    ROUND((frequency / (end - init)), 2) AS order_density
    
FROM SUMMARY_TABLE              -- Source: Three-tiered interval classification from step 8.18

-- STRATEGIC ORDERING: Present intervals in logical priority sequence
-- LARGE (1): Comprehensive optimization approach first
-- MEDIUM (2): Focused high-impact approach second  
-- SMALL (3): Quick-win immediate approach third
-- BUSINESS VALUE: Progressive options from broad strategic to targeted tactical
ORDER BY CASE interval_type 
    WHEN 'LARGE' THEN 1           -- Comprehensive strategic approach
    WHEN 'MEDIUM' THEN 2          -- Focused high-impact approach
    WHEN 'SMALL' THEN 3           -- Quick-win tactical approach
END

-- FINAL RESULT: Three strategic ECR optimization intervals with comprehensive business metrics
-- STRATEGIC OUTPUT: Actionable payment ranges for targeted cost optimization initiatives
-- BUSINESS VALUE: Multiple approaches from comprehensive market impact to focused quick wins;
-- =============================================================================
-- ===== MATHEMATICAL FOUNDATION: BAYESIAN KNUTH METHOD FOR ECR ANALYSIS =====
-- =============================================================================
/*
 * THEORETICAL BASIS: Two-Stage Bayesian Knuth Method for ECR Distribution Analysis
 * 
 * 1. FOUNDATION OF THE KNUTH METHOD FOR OPTIMAL HISTOGRAM BINNING:
 *    The Knuth method uses Bayesian evidence to find the optimal
 *    number of bins M* that maximizes the marginal likelihood function.
 *    This avoids the over-binning problems of the Freedman-Diaconis method
 *    with large-scale financial data and asymmetric ECR distributions.
 *    
 *    F(M) = N ln(M) + ln Γ(M/2) - M ln Γ(1/2) - ln Γ((N+M)/2) + Σₖ ln Γ(nₖ + 1/2)
 *    
 *    Where:
 *    - N = total number of approved order observations
 *    - M = candidate bin count
 *    - nₖ = number of observations in bin k
 *    - Γ = gamma function
 *
 * 2. ECR-Focused Analysis Approach:
 *    This query analyzes the distribution of ORDER_PROCESSOR_FEE_USD across PAYMENT_AMOUNT_USD ranges:
 *    - **X-axis**: Payment amount in USD (order total value)
 *    - **Y-axis**: ECR values (ORDER_PROCESSOR_FEE_USD)
 *    - **Objective**: Find payment amount intervals with highest ECR concentration
 *    - **Filtering criterion**: 90% of total ECR (not 90% of order count)
 *
 * 3. Logarithmic Transformation for Financial Data:
 *    We apply y = ln(PAYMENT_AMOUNT_USD) to:
 *    - **Stabilize variance**: Reduces the impact of right-tail outliers in payment amounts
 *    - **Uniform width**: Bins in log scale have constant width
 *    - **Improve convergence**: Bayesian optimization is more stable
 *    - **Multiplicative interpretation**: Ranges represent proportional amount ranges
 *
 * 4. Pre-aggregation in Micro-bins (Computational Efficiency):
 *    For scalability in Snowflake with millions of approved orders:
 *    - **Step 1**: Divide log range into U=20,000 uniform micro-bins
 *    - **Step 2**: Count observations and sum ECR per micro-bin (single data pass)
 *    - **Step 3**: Re-aggregate micro-bins to each M candidate: k = ⌊u × M / U⌋
 *    - **Advantage**: We evaluate M=64 to M=4096 without touching original data
 *
 * 5. ECR-Based Filtering Strategy:
 *    Unlike frequency-based filtering, we prioritize ECR concentration:
 *    - **Stage 1**: Log-scale analysis, keep bins with top 90% of total ECR
 *    - **Stage 2**: Linear-scale analysis on filtered data, keep bins with top 90% of remaining ECR
 *    - **Result**: ~81% of original ECR concentrated in core payment amount ranges
 *    - **Final selection**: Longest continuous sequence with highest total ECR
 *
 * 6. Geometric Grid of M Candidates:
 *    M = round(M_MIN × M_STEP^t) where M_STEP = 1.03
 *    - **Range**: M ∈ [64, 4096] (balancing granularity and efficiency)
 *    - **Geometric progression**: Explores the space efficiently
 *    - **No duplicates**: DISTINCT guarantees unique evaluation per M
 *
 * 7. Interpretation of Bayesian Score F(M):
 *    - **Maximization**: M* = argmax F(M) optimally balances bias-variance
 *    - **Complexity component**: N ln(M) penalizes excess bins
 *    - **Fit component**: Σₖ ln Γ(nₖ + 1/2) rewards well-populated bins
 *    - **Bayesian regularization**: Gamma terms implement informative prior
 *
 * 8. Final Output Metrics:
 *    - **ECR Density**: total_ecr / interval_width (ECR concentration per USD)
 *    - **ECR Percentage**: Fraction of total ECR contained in interval
 *    - **Order Density**: orders / interval_width (order concentration per USD)
 *    - **Interval Types**: LARGE (full range), MEDIUM (80% ECR), SMALL (highest ECR bin)
 *
 * 9. Advantages for ECR Analysis:
 *    - **Cost optimization focus**: Identifies payment ranges with highest processing costs
 *    - **Strategic targeting**: Enables focused cost reduction efforts
 *    - **Scalability**: Pre-aggregation allows efficient handling of Big Data
 *    - **Statistical robustness**: Bayesian approach handles ECR distribution complexity
 */