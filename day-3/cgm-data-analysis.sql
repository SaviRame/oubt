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
--------------------------------------------------------------------------------------------
--2.Identify master data domains in NYC taxi dataset with business justification

''' A)Taxi trips are transactional data because they record events.

	B)Taxi zones are master data because:
		1.They represent real places
		2.They don’t change often
		3.They are used by every trip
	C)Payment type and rate code are reference data because they are fixed lists.'''
-----------------------------------------------------------------------------------------------
-- 3. Data dictionary
'''
| Column Name           | Description                      | Business Owner | Data Steward    | Sensitivity Level | Retention Policy | Quality Rules                  |
| --------------------- | -------------------------------- | -------------- | --------------- | ----------------- | ---------------- | ------------------------------ |
| VendorID              | Taxi service provider identifier | NYC Taxi 
																Authority   | Data operations | Low               | 7 years          | Must not be NULL; valid vendor |
| PULocationID          | Pickup taxi zone ID              | NYC TLC        | 				  | Low               | Permanent        | Must exist in zone lookup      |
| DOLocationID          | Dropoff taxi zone ID             | NYC TLC        | Geo Data Team   | Low               | Permanent        | Must exist in zone lookup      |
| tpep_pickup_datetime  | Trip start time                  | NYC TLC        | Data Ops Team   | Medium            | 7 years          | Must be valid timestamp        |
| tpep_dropoff_datetime | Trip end time                    | NYC TLC        | Data Ops Team   | Medium            | 7 years          | Must be after pickup time      |
| Fare_amount           | Base fare amount                 | Finance Dept   | Billing Team    | Medium            | 7 years          | Must be ≥ 0                    |'''
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------


  