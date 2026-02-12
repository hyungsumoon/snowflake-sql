-- 분석 샘플: 간단한 집계 예시
WITH sample AS (
  SELECT seq4() % 10 AS bucket, rand() AS val FROM table(generator(rowcount => 1000))
)
SELECT bucket, approx_percentile(val, 0.5) AS median_val FROM sample GROUP BY bucket;
