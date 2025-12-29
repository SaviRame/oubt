----AGGREGATION
--Calculate the patient-wise minimum, maximum, and average glucose values.
--Patient wise average, min, max glucose values 

SELECT patientid, 
	   round(AVG(glucosevaluemgdl),2), 
	   MIN(glucosevaluemgdl),
	   MAX(glucosevaluemgdl) as max_glucose
FROM dexcom
GROUP BY patientid
ORDER BY max_glucose desc;
----------------------------------------------------------------------------
--JOIN
-- Calculate gender wise average glucose and number of readings per gender.
-- Gender wise average glucose

SELECT gender,COUNT(gender) AS reading_count, ROUND(AVG(glucosevaluemgdl),2) AS avg_glucose
FROM demography d JOIN dexcom dx ON d.patientid = dx.patientid
GROUP BY d.gender
ORDER BY avg_glucose;
--------------------------------------------------------------------------------
--WINDOW FUNCTIONS
--Rank patients by blood glucose level within each gender group on a randomly selected day

SELECT
d.patientid,
  d.gender,
dx.glucosevaluemgdl ,
  RANK() OVER ( PARTITION BY gender
    ORDER BY glucosevaluemgdl DESC
  ) AS glucose_rank
  FROM public.dexcom dx,public.demography d 
  WHERE dx.patientid=d.patientid AND timeof >= '2020-02-13':: DATE
  AND timeof <'2020-02-13':: DATE + INTERVAL '1 day' ;
--------------------------------------------------------------------------------------
--CTE FUNCTION EXAMPLE
WITH food_cte AS (
    SELECT
        patientid,
        timeof,
        logged_food,
        calorie,
        total_carb
    FROM foodlog
)
SELECT *
FROM food_cte
WHERE patientid = 1
ORDER BY timeof;
--------------------------------------------------------------------------------------------
  
  