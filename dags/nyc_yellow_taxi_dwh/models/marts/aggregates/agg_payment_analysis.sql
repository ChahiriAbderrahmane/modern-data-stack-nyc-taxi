{{ config( materialized='view', schema='gold') }}

-- Analyse des méthodes de paiement et de la générosité (Pourboires)
WITH fact_enriched AS (
    SELECT
        f.total_amount,        -- Montant calculé propre
        f.tip_amount_usd,
        f.tip_percentage,
        f.trip_distance,
        f.trip_duration_minutes,
        f.payment_type_id,
        d.year AS pickup_year,
        d.month AS pickup_month,
        tc.fare_category
    FROM {{ ref('fact_taxi_trips_v2') }} f
    LEFT JOIN {{ ref('dim_date') }} d 
        ON f.pickup_date_id = d.date_id
    LEFT JOIN {{ ref('dim_trip_category') }} tc 
        ON f.trip_category_key = tc.trip_category_key
)

SELECT
    payment_type_id,
    CASE payment_type_id
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        ELSE 'Unknown/Other'
    END AS payment_type_description,
    pickup_year,
    pickup_month,
    -- ANALYSE DE VOLUME & PART DE MARCHÉ
    COUNT(*) AS total_trips,
    -- Formule Magique : (Volume de ce paiement / Volume Total du Mois) * 100
    ROUND(
        COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER (PARTITION BY pickup_year, pickup_month) * 100
    , 2) AS payment_method_share_pct,
    -- REVENUS
    ROUND(SUM(total_amount)::NUMERIC, 2) AS total_revenue_usd,
    ROUND(AVG(total_amount)::NUMERIC, 2) AS avg_fare_usd,
    -- ANALYSE DES POURBOIRES (Tips)
    -- Indispensable pour voir si le Cash génère vraiment 0 tip (Data Quality Issue connue)
    ROUND(SUM(tip_amount_usd)::NUMERIC, 2) AS total_tips_usd,
    ROUND(AVG(tip_amount_usd)::NUMERIC, 2) AS avg_tip_usd,
    ROUND(AVG(tip_percentage)::NUMERIC, 2) AS avg_tip_percentage,
    
    -- L'intérêt de la colonne : Elle sert à prouver aux Analystes qu'ils ne doivent JAMAIS utiliser les trajets en Cash 
    -- pour calculer la générosité moyenne des New-Yorkais, sinon ils vont sous-estimer massivement le résultat.
    -- car il se peut que le chauffeur met le tip dans sa poche et appuie sur "Fin de course", et donc on a pas de traçabilité.

    -- on peut avoir ce réflexe: "Je sais que les gens donnent environ 18% de pourboire quand ils paient par carte. 
    -- donc je vais supposer qu'ils donnent la même chose en cash."


    -- CARACTÉRISTIQUES DU TRAJET (Profilage)
    -- Est-ce que les gens paient en Cash pour les petits trajets ?
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_distance_miles,
    ROUND(AVG(trip_duration_minutes)::NUMERIC, 2) AS avg_duration_minutes,
    
    -- 5. SEGMENTATION PAR MONTANT 
    -- Cette info vient de la jointure dim_trip_categories
    COUNT(CASE WHEN fare_category = 'Low (< $10)' THEN 1 END) AS low_fare_trips,
    COUNT(CASE WHEN fare_category = 'Medium ($10-$25)' THEN 1 END) AS medium_fare_trips,
    COUNT(CASE WHEN fare_category = 'High ($25-$50)' THEN 1 END) AS high_fare_trips,
    COUNT(CASE WHEN fare_category = 'Very High (> $50)' THEN 1 END) AS very_high_fare_trips

FROM fact_enriched

GROUP BY 
    payment_type_id,
    CASE payment_type_id
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        ELSE 'Unknown/Other'
    END,
    pickup_year,
    pickup_month,
    fare_category -- Nécessaire si utilisé dans le SELECT/CASE, mais ici on compte les cas, donc pas besoin dans le Group By principal si on agrège.
                  -- CORRECTION : fare_category est utilisé DANS une agrégation (COUNT CASE), donc on ne groupe PAS par fare_category.
                  -- On groupe uniquement par les dimensions d'affichage.

ORDER BY 
    pickup_year DESC,
    pickup_month DESC,
    total_trips DESC