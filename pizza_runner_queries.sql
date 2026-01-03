-- ** Pizza Metrics

-- How many pizzas were ordered?

select count(pizza_id) total_orders
from `pizza_runner.customers_order`

-- How many unique customer orders were made?
select count(distinct order_id) AS total_unique_order
from `pizza_runner.customers_order`


-- How many successful orders were delivered by each runner?
select runner_id, count(order_id) orders_delivered
from `pizza_runner.runner_orders`
where cancellation is null or cancellation = "NULL" or cancellation = "null"
group by runner_id;


select runner_id, count(order_id) orders_delivered
from `pizza_runner.runner_orders`
WHERE cancellation IS NULL OR LOWER(cancellation) not LIKE '%cance%'
group by runner_id;


select *
from `pizza_runner.runner_orders`

-- How many of each type of pizza was delivered?

select co.pizza_id, count(co.order_id) pizza_delivered
from `pizza_runner.customers_order` co
join `pizza_runner.runner_orders` ro
on co.order_id = ro.order_id
WHERE ro.cancellation IS NULL OR LOWER(ro.cancellation) not LIKE '%cance%'
group by pizza_id

-- How many Vegetarian and Meatlovers were ordered by each customer?
select customer_id, pizza_name, count(order_id) total_orders
from `pizza_runner.customers_order` co
join `pizza_runner.pizza_name` pn
  on co.pizza_id = pn.pizza_id
group by pizza_name, customer_id

-- What was the maximum number of pizzas delivered in a single order? pending
with CTE as 
(select co.order_id, count(co.pizza_id) pizzas_delivered
from `pizza_runner.customers_order` co
join `pizza_runner.runner_orders` ro
on co.order_id = ro.order_id
WHERE ro.cancellation IS NULL OR LOWER(ro.cancellation) not LIKE '%cance%'
group by order_id)
select max(pizzas_delivered) 
from CTE 


with CTE as 
(select co.order_id, count(co.pizza_id) pizza_delivered
from `pizza_runner.customers_order` co
join `pizza_runner.runner_orders` ro
on co.order_id = ro.order_id
WHERE ro.cancellation IS NULL OR LOWER(ro.cancellation) not LIKE '%cance%'
group by order_id),
max_p as (select max(pizza_delivered) as pizza_delivered
from CTE) 
select c.order_id, m.pizza_delivered
from CTE c
join max_p m
on c.pizza_delivered = m.pizza_delivered



-- For each customer, how many delivered pizzas had at least 1 change and how many had no changes?

with CTE as 
(select co.customer_id, 
            COUNT(
      CASE 
        WHEN (
          (exclusions IS NOT NULL AND LOWER(exclusions) != 'null') OR 
          (extras IS NOT NULL AND LOWER(extras) != 'null')
        ) THEN 1
      END
    ) AS changed_pizzas,
    COUNT(
      CASE 
        WHEN (
          (exclusions IS NULL OR LOWER(exclusions) = 'null') AND 
          (extras IS NULL OR LOWER(extras) = 'null')
        ) THEN 1
      END
    ) AS unchanged_pizzas
from `pizza_runner.customers_order` co
join `pizza_runner.runner_orders` ro
on co.order_id = ro.order_id
WHERE ro.cancellation IS NULL OR LOWER(ro.cancellation) not LIKE '%cance%'
group by co.customer_id)
select  customer_id, 
        changed_pizzas,
        unchanged_pizzas
from CTE



-- How many pizzas were delivered that had both exclusions and extras?

with CTE as 
(select co.customer_id, 
            COUNT(
      CASE 
        WHEN (
          (exclusions IS NOT NULL AND LOWER(exclusions) != 'null') AND 
          (extras IS NOT NULL AND LOWER(extras) != 'null')
        ) THEN 1
      END
    ) AS changed_pizzas
from `pizza_runner.customers_order` co
join `pizza_runner.runner_orders` ro
on co.order_id = ro.order_id
WHERE ro.cancellation IS NULL OR LOWER(ro.cancellation) not LIKE '%cance%'
group by co.customer_id)
select  customer_id, 
        changed_pizzas
        -- unchanged_pizzas
from CTE



select *
from `pizza_runner.customers_order` co

-- What was the total volume of pizzas ordered for each hour of the day?


select extract( hour from order_time) as time_per_hour, count(*)
from `pizza_runner.customers_order` co
group by extract( hour from order_time)
order by time_per_hour


-- What was the volume of orders for each day of the week?


select extract( DAYOFWEEK  from order_time) as week_days, count(distinct order_id) Per_day_orders
from `pizza_runner.customers_order` 
group by extract( DAYOFWEEK  from order_time)
order by week_days


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- ** Runner and Customer Experience

-- How many runners signed up for each 1 week period? (i.e. week starts 2021-01-01)

--this query gives you both week number in year and week start date 
select  date_trunc(registration_date, week) Weeks_date,
        Extract(week from registration_date) Weeks,
        count(distinct runner_id)
from `pizza_runner.runner`
group by Weeks_date, Weeks

-- Answer with date start of week and count of registered users

select  date_trunc(registration_date, week) Weeks_date,
        count(distinct runner_id)registered_runners 
from `pizza_runner.runner`
group by Weeks_date


select  *
from `pizza_runner.runner_orders`


-- What was the average time in minutes it took for each runner to arrive at the Pizza Runner HQ to pickup the order?


select *
from `pizza_runner.runner_orders`

select *
from `pizza_runner.customers_order`

with CTE as 
(SELECT *,
  CASE 
    WHEN pickup_time IS NULL OR pickup_time = 'null' THEN NULL
    ELSE PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', pickup_time)
  END AS parsed_pickup_time
from `pizza_runner.runner_orders`)
SELECT 
  c.runner_id,
  ROUND(AVG(TIMESTAMP_DIFF(c.parsed_pickup_time, co.order_time, MINUTE)), 2) AS avg_time_to_reach_pickup
FROM 
  CTE c
JOIN 
  `pizza_runner.customers_order` co
ON 
  c.order_id = co.order_id
GROUP BY 
  c.runner_id
ORDER BY 
  c.runner_id;

-- Is there any relationship between the number of pizzas and how long the order takes to prepare?

with CTE as 
(SELECT *,
  CASE 
    WHEN pickup_time IS NULL OR pickup_time = 'null' THEN NULL
    ELSE PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', pickup_time)
  END AS parsed_pickup_time
from `pizza_runner.runner_orders`),
abc as (
  SELECT 
  count(co.pizza_id) pizza_count,
  ROUND(AVG(TIMESTAMP_DIFF(c.parsed_pickup_time, co.order_time, MINUTE)), 2) AS avg_time_
FROM 
  CTE c
JOIN 
  `pizza_runner.customers_order` co
ON 
  c.order_id = co.order_id
GROUP BY 
co.order_id
)
select  pizza_count, 
        avg(avg_time_)
  from abc
group BY pizza_count



-- What was the average distance travelled for each customer?

select *
  from `pizza_runner.runner_orders` r join `pizza_runner.customers_order` c
    using(order_id)
group by order_id


select    order_id, 
          Avg(CASE 
          WHEN distance IS NULL OR distance = 'null' THEN NULL
          ELSE CAST(REGEXP_REPLACE(distance, '[^0-9.]', '') AS FLOAT64) 
          END) AS distance_in_km
  from `pizza_runner.runner_orders` r join `pizza_runner.customers_order` c
    using(order_id)
group by order_id


-- What was the difference between the longest and shortest delivery times for all orders?

with CTE as 
(SELECT *,
  CASE 
    WHEN pickup_time IS NULL OR pickup_time = 'null' THEN NULL
    ELSE PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', pickup_time)
  END AS parsed_pickup_time
from `pizza_runner.runner_orders` join `pizza_runner.customers_order` c
    using(order_id))  ,
adc as (select *,
        extract(minute  FROM parsed_pickup_time) pickup_minutes,
        CASE 
          WHEN duration IS NULL OR duration = 'null' THEN NULL
          ELSE CAST(REGEXP_REPLACE(duration, '[^0-9.]', '') AS FLOAT64) 
          END AS duration_in_min
  from CTE
)
select order_id, 
        avg(pickup_minutes - duration_in_min)
from adc
group by order_id


-- What was the difference between the longest and shortest delivery times for all orders?

with CTE as 
(SELECT *,
  CASE 
  WHEN duration IS NULL OR duration = 'null' THEN NULL
  ELSE CAST(REGEXP_REPLACE(duration, '[^0-9.]', '') AS FLOAT64) 
  END AS duration_in_min
from `pizza_runner.runner_orders` 
)
select min(duration_in_min) Min_time,
        max(duration_in_min) Max_time
    from CTE;


-- What was the average speed for each runner for each delivery and do you notice any trend for these values?


with CTE as 
(SELECT *,
  CASE 
  WHEN duration IS NULL OR duration = 'null' THEN NULL
  ELSE CAST(REGEXP_REPLACE(duration, '[^0-9.]', '') AS FLOAT64) 
  END AS duration_in_min,
  CASE 
  WHEN distance IS NULL OR distance = 'null' THEN NULL
  ELSE CAST(REGEXP_REPLACE(distance, '[^0-9.]', '') AS FLOAT64) 
  END Distance_ofrunner
from `pizza_runner.runner_orders` 
)
select  runner_id,
        -- duration_in_min,
        -- Distance_ofrunner,
        round(Avg(Distance_ofrunner/(duration_in_min/60)),2) as speed 
from CTE
WHERE cancellation IS NULL OR LOWER(cancellation) not LIKE '%cance%'
group by runner_id
order by speed


-- What is the successful delivery percentage for each runner?

with CTE as (
  select count(*) total_count
  from `pizza_runner.runner_orders`
  WHERE cancellation IS NULL OR LOWER(cancellation) not LIKE '%cance%'
),
abc as (select runner_id, count(*) per_runner
from `pizza_runner.runner_orders`
  WHERE cancellation IS NULL OR LOWER(cancellation) not LIKE '%cance%'
group by runner_id)
select  a.runner_id,
        a.per_runner,
        c.total_count,
        round((a.per_runner/c.total_count) * 100, 2) as percentage 
from CTE c, abc a 
ORDER BY percentage DESC;


select runner_id, count(runner_id) 
from `pizza_runner.runner_orders`


-- What are the standard ingredients for each pizza?


Select  pn.pizza_id,  
        pn.pizza_name, 
        STRING_AGG(pt.topping_name order by pt.topping_name) as ingredients
From `pizza_runner.pizza_name` pn join `pizza_runner.pizza_recipes` pr
    on pn.pizza_id = pr.pizza_id
    JOIN UNNEST(SPLIT(pr.toppings, ',')) AS topping_id
    JOIN pizza_runner.pizza_toppings pt ON CAST(topping_id AS INT64) = pt.topping_id
group by pizza_id, pizza_name    
order by pizza_id    


SELECT 
  pn.pizza_id,  
  pn.pizza_name, 
  STRING_AGG(pt.topping_name ORDER BY pt.topping_name) AS ingredients
FROM `pizza_runner.pizza_name` pn
JOIN `pizza_runner.pizza_recipes` pr ON pn.pizza_id = pr.pizza_id
JOIN UNNEST(SPLIT(pr.toppings, ',')) AS topping_id
JOIN `pizza_runner.pizza_toppings` pt ON CAST(topping_id AS INT64) = pt.topping_id
GROUP BY pn.pizza_id, pn.pizza_name    
ORDER BY pn.pizza_id;

create view first-project-449916.pizza_runner.topping_id_name as
select  pn.pizza_id, 
        pn.pizza_name,
        pr.toppings,
        pt.topping_id,
        pt.topping_name
FROM `pizza_runner.pizza_name` pn
JOIN `pizza_runner.pizza_recipes` pr ON pn.pizza_id = pr.pizza_id
JOIN UNNEST(SPLIT(pr.toppings, ',')) AS topping_id
JOIN `pizza_runner.pizza_toppings` pt ON CAST(topping_id AS INT64) = pt.topping_id


-- What was the most commonly added extra?


with CTE as 
(select tn.topping_id,
        tn.topping_name,
        count(*) as extra_count
from `pizza_runner.customers_order` co
  cross join unnest(split(co.extras, ', ')) as extra_id
  join `pizza_runner.topping_id_name` tn
    on cast(extra_id as int) = tn.topping_id
where co.extras is not null 
  and lower(co.extras) != 'null' 
  and lower(co.extras) not like '%null%'
group by tn.topping_id,tn.topping_name)
select topping_id,
        topping_name,
        max(extra_count) extra_counts
from CTE        
group by topping_id,
        topping_name


-- What was the most common exclusion?

with CTE as 
(select tn.topping_id,
        tn.topping_name,
        count(*) as exclusions_count
from `pizza_runner.customers_order` co
  cross join unnest(split(co.exclusions, ', ')) as exclusions_id
  join `pizza_runner.topping_id_name` tn
    on cast(exclusions_id as int) = tn.topping_id
where co.exclusions is not null 
  and lower(co.exclusions) != 'null' 
  and lower(co.exclusions) not like '%null%'
group by tn.topping_id,tn.topping_name),
ranking as (
select topping_id,
        topping_name,
        -- max(exclusions_count) exclusions_counts
        exclusions_count,
        rank() over (order by exclusions_count desc) rnk
from CTE)      
select  topping_id,
        topping_name, 
        exclusions_count
from ranking 
where rnk = 1

-- Generate an order item for each record in the customers_orders table in the format of one of the following:
-- Meat Lovers
-- Meat Lovers - Exclude Beef
-- Meat Lovers - Extra Bacon
-- Meat Lovers - Exclude Cheese, Bacon - Extra Mushroom, Peppers



with CTE1 as
(
  select  co.order_id,
          string_agg('Exclude ' || ti.topping_name, ', ') AS exclusions
from `pizza_runner.customers_order` co 
      cross join unnest(split(co.exclusions, ', ')) as exclusions_id 
      -- cross join unnest(split(co.extras, ', ')) as extra_id
join `pizza_runner.topping_id_name` ti
  ON CAST(exclusions_id AS INT) = ti.topping_id
where co.exclusions is not null 
  and lower(co.exclusions) != 'null' 
  and lower(co.exclusions) not like '%null%'
GROUP BY co.order_id, co.customer_id, ti.pizza_name 
),
CTE2 as 
(
  select  co.order_id,
          STRING_AGG('Extra ' || ti.topping_name, ', ') AS extras
from `pizza_runner.customers_order` co 
      -- cross join unnest(split(co.exclusions, ', ')) as exclusions_id 
      cross join unnest(split(co.extras, ', ')) as extra_id
join `pizza_runner.topping_id_name` ti
  ON CAST(extra_id AS INT) = ti.topping_id
where co.extras is not null 
  and lower(co.extras) != 'null' 
  and lower(co.extras) not like '%null%'
group by co.order_id
)
select  a.order_id,
        a.customer_id,
        a.pizza_name,
        a.exclusions,
        b.extras
from CTE1 as a join CTE2 as b on a.order_id = b.order_id 
GROUP BY a.order_id, a.customer_id, a.pizza_name




WITH CTE1 AS (
  SELECT 
    co.order_id,
    STRING_AGG('Exclude ' || ti.topping_name, ', ') AS exclusions
  FROM `pizza_runner.customers_order` co 
  CROSS JOIN UNNEST(SPLIT(IFNULL(co.exclusions, ''), ', ')) AS exclusions_id 
  JOIN `pizza_runner.topping_id_name` ti
    ON SAFE_CAST(exclusions_id AS INT64) = ti.topping_id
  WHERE co.exclusions IS NOT NULL 
    AND LOWER(co.exclusions) != 'null' 
    AND LOWER(co.exclusions) NOT LIKE '%null%'
  GROUP BY co.order_id
),

CTE2 AS (
  SELECT 
    co.order_id,
    STRING_AGG('Extra ' || ti.topping_name, ', ') AS extras
  FROM `pizza_runner.customers_order` co 
  CROSS JOIN UNNEST(SPLIT(IFNULL(co.extras, ''), ', ')) AS extra_id 
  JOIN `pizza_runner.topping_id_name` ti
    ON SAFE_CAST(extra_id AS INT64) = ti.topping_id
  WHERE co.extras IS NOT NULL 
    AND LOWER(co.extras) != 'null' 
    AND LOWER(co.extras) NOT LIKE '%null%'
  GROUP BY co.order_id
)

SELECT
  co.order_id,
  co.customer_id,
  ti.pizza_name,
  c1.exclusions,
  c2.extras,
  CASE
    WHEN c1.exclusions IS NOT NULL AND c2.extras IS NOT NULL THEN 
      CONCAT(ti.pizza_name, ' - ', c1.exclusions, ' - ', c2.extras)
    WHEN c1.exclusions IS NOT NULL THEN 
      CONCAT(ti.pizza_name, ' - ', c1.exclusions)
    WHEN c2.extras IS NOT NULL THEN 
      CONCAT(ti.pizza_name, ' - ', c2.extras)
    ELSE 
      ti.pizza_name
  END AS order_item
FROM `pizza_runner.customers_order` co
LEFT JOIN CTE1 c1 ON co.order_id = c1.order_id
LEFT JOIN CTE2 c2 ON co.order_id = c2.order_id
JOIN `pizza_runner.topping_id_name` ti ON co.pizza_id = ti.pizza_id
GROUP BY co.order_id, co.customer_id, ti.pizza_name, c1.exclusions, c2.extras
ORDER BY co.order_id;



-- Generate an alphabetically ordered comma separated ingredient list for each pizza order from the customer_orders table and add a 2x in front of any relevant ingredients


with CTE as (select order_id,
        topping_name,
        count(*) as topping_Count 
from  (SELECT 
    co.order_id,
    ti.topping_name
  FROM `pizza_runner.customers_order` co
  JOIN `pizza_runner.topping_id_name` ti
    ON co.pizza_id = ti.pizza_id

    union all

SELECT    co.order_id,
          tn.topping_name
        -- string_agg(topping_name, ', ' order by topping_name) toppings,
from `pizza_runner.customers_order`  co
      CROSS JOIN UNNEST(SPLIT(IFNULL(co.exclusions, ''), ', ')) AS exclusions_id 
      CROSS JOIN UNNEST(SPLIT(IFNULL(co.extras, ''), ', ')) AS extra_id 
      join `pizza_runner.topping_id_name` tn
          ON safe_CAST(extra_id AS INT) = tn.topping_id
WHERE co.extras IS NOT NULL)
group by order_id, topping_name)

select  order_id,
        string_agg(
                      Case when CTE.topping_Count = 2 then  '2x ' || topping_name
                      else topping_name
                      end,
                      ', ' order by topping_name 
        ) as order_item
from CTE
group by order_id
order by order_id


-- What is the total quantity of each ingredient used in all delivered pizzas sorted by most frequent first?


WITH CTE AS (
  -- First subquery: Select order_id, topping_name, and count
  SELECT order_id,
         topping_name,
         COUNT(*) AS topping_Count
  FROM (
    -- First part of the UNION ALL
    SELECT co.order_id,
           ti.topping_name
    FROM `pizza_runner.customers_order` co
    JOIN `pizza_runner.topping_id_name` ti
      ON co.pizza_id = ti.pizza_id

    UNION ALL

    -- Second part of the UNION ALL
    SELECT co.order_id,
           tn.topping_name
    FROM `pizza_runner.customers_order` co
    -- CROSS JOIN UNNEST(SPLIT(IFNULL(co.exclusions, ''), ', ')) AS exclusions_id
    CROSS JOIN UNNEST(SPLIT(IFNULL(co.extras, ''), ', ')) AS extra_id
    JOIN `pizza_runner.topping_id_name` tn
      ON SAFE_CAST(extra_id AS INT64) = tn.topping_id
    WHERE co.extras IS NOT NULL
  )
  GROUP BY order_id, topping_name
)
select CTE.topping_name,
  SUM(CTE.topping_count) AS total_quantity_used
FROM CTE
-- Join to ensure only delivered orders are included
JOIN `pizza_runner.runner_orders` ro 
  ON CTE.order_id = ro.order_id
WHERE ro.cancellation IS NULL 
  OR LOWER(ro.cancellation) NOT LIKE '%cance%'
GROUP BY CTE.topping_name
ORDER BY total_quantity_used DESC;


-- What are the standard ingredients for each pizza?

SELECT pizza_id, string_agg(topping_name, ', ' order by topping_name) toppings
from `pizza_runner.topping_id_name`
group by pizza_id


-- What was the most commonly added extra?
select  co.pizza_id,  
        string_agg(distinct extras_id, ', ') Extra_id,
        string_agg(distinct topping_name, ', ') topping_name
from `pizza_runner.customers_order` co
    cross join unnest(split(co.extras, ', ')) extras_id
  join `pizza_runner.topping_id_name` tn    
    on safe_cast(extras_id as int) =   tn.topping_id
where co.extras is not null and  lower(co.extras) != 'null' 
  and lower(co.extras) not like '%null%'
group by co.pizza_id


with CTE as
(
select  tn.topping_id,
        tn.topping_name,
        count(*) times_added
from `pizza_runner.customers_order` co
    cross join unnest(split(co.extras, ', ')) extras_id
  join `pizza_runner.topping_id_name` tn    
    on safe_cast(extras_id as int) =   tn.topping_id
where co.extras is not null and  lower(co.extras) != 'null' 
  and lower(co.extras) not like '%null%'
group by tn.topping_name, tn.topping_id
),
mostly_added as
(
  select  *,
          dense_rank() over (order by times_added desc) mostly_addded
  from CTE
)
select * 
from mostly_added
where mostly_addded = 1;

-- What was the most common exclusion?

with CTE as
(
select  tn.topping_id,
        tn.topping_name,
        count(*) times_removed
from `pizza_runner.customers_order` co
    cross join unnest(split(co.exclusions, ', ')) exclusion_id
  join `pizza_runner.topping_id_name` tn    
    on safe_cast(exclusion_id as int) =   tn.topping_id
where co.exclusions is not null and  lower(co.exclusions) != 'null' 
  and lower(co.exclusions) not like '%null%'
group by tn.topping_name, tn.topping_id
),
mostly_removed as
(
  select  *,
          dense_rank() over (order by times_removed desc) mostly_reemoved
  from CTE
)
select * 
from mostly_removed
where mostly_reemoved = 1;









with CTE 
(select * 
from `pizza_runner.customers_order` co
    cross join unnest(split(co.exclusions, ', ')) exclusion_id
  join `pizza_runner.topping_id_name` tn
    on safe_cast(exclusion_id as int) = tn.topping_id 
where co.exclusions is not null 
  and lower(co.exclusions) != 'null' 
  and lower(co.exclusions) not like '%null%'

union all

select * 
from `pizza_runner.customers_order` co
    cross join unnest(split(co.extras, ', ')) extras_id
  join `pizza_runner.topping_id_name` tn    
    on safe_cast(extras_id as int) =   tn.topping_id
where co.extras is not null and  lower(co.extras) != 'null' 
  and lower(co.extras) not like '%null%'
)
select * from CTE
where exclusions is not null 
  and lower(exclusions) != 'null' 
  and lower(exclusions) not like '%null%'
and extras is not null and  lower(extras) != 'null' 
  and lower(extras) not like '%null%'


-- **** D. Pricing and Ratings ****
-- If a Meat Lovers pizza costs $12 and Vegetarian costs $10 and there were no charges for changes - how much money has Pizza Runner made so far if there are no delivery fees?

With CTE as 
(
select  pizza_id, 
        count(pizza_id) total_delivered_pizza,
        case 
        when pizza_id = 1 then  12
        else  10
        end Price,
        case 
        when pizza_id = 1 then count(pizza_id) *  12
        else count(pizza_id) *  10
        end Total_income
from pizza_runner.customers_order co
join pizza_runner.runner_orders rn 
    on co.order_id = rn.order_id
where rn.cancellation is null or lower(rn.cancellation) NOT LIKE '%cance%'
group by pizza_id
) 
Select SUM(Total_income)
from CTE


-- What if there was an additional $1 charge for any pizza extras?

with CTE as 
(
select  co.order_id, 
        (case 
        when pizza_id = 1 then  12
        else  10
        end + 
        case 
        when cross join unnest(split(co.extras, ', '))  is not null then 1
        else 0
        end) total_income
from pizza_runner.customers_order co
      cross join unnest(split(co.extras, ', ')) extrass
join pizza_runner.runner_orders rn 
    on co.order_id = rn.order_id
where rn.cancellation is null or lower(rn.cancellation) NOT LIKE '%cance%'
)
select sum(total_income) total_income
from CTE 



with base_price as 
(
select  co.order_id, 
        case 
        when pizza_id = 1 then  12
        else  10
        end base_price 
from `pizza_runner.customers_order` co
join pizza_runner.runner_orders rn 
    on co.order_id = rn.order_id
where rn.cancellation is null or lower(rn.cancellation) NOT LIKE '%cance%'
),
extras_price as 
(select order_id,
        count(extrass) price_extra
from `pizza_runner.customers_order` co
      cross join unnest(split( COALESCE(co.extras, ''), ',')) extrass
  GROUP BY order_id
),
Per_pizza_revenue as 
(
select  b.order_id, 
        b.base_price +  e.price_extra total_price
from base_price b left join extras_price e 
on b.order_id = e.order_id 
)
select sum(total_price) total_revenue 
from Per_pizza_revenue



    order_id,
    COUNT(*) AS extras_count


-- Add cheese is $1 extra

with base_price as 
(
select  co.order_id, 
        case 
        when pizza_id = 1 then  12
        else  10
        end base_price 
from `pizza_runner.customers_order` co
left join pizza_runner.runner_orders rn 
    on co.order_id = rn.order_id
where rn.cancellation is null or lower(rn.cancellation) NOT LIKE '%cance%'
),
extra_for_cheesee as
(
  select  co.order_id,
          Max(case 
          when pt.topping_name = 'Cheese' then 1 
          else 0
          end) extra_for_cheese
from `pizza_runner.customers_order` co
      cross join unnest(split( COALESCE(co.extras, ''), ',')) extras_id
      left join `pizza_runner.pizza_toppings` pt
      on safe_cast(extras_id as int64) = pt.topping_id
group by co.order_id
),
 Per_pizza_revenue as 
(
select  b.order_id, 
        b.base_price +  COALESCE(e.extra_for_cheese,0) total_price
from base_price b left join extra_for_cheesee e 
on b.order_id = e.order_id 
)
select sum(total_price) total_revenue 
from Per_pizza_revenue

