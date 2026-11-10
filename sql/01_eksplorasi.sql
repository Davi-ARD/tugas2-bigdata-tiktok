-- Eksplorasi awal dataset TikTok (batch 01, 15.000 baris)
-- Sumber: workspace.tiktok_lake.tiktok_raw_eksplorasi

-- 1. Jumlah video per claim_status
SELECT claim_status, COUNT(*) AS jumlah_video
FROM workspace.tiktok_lake.tiktok_raw_eksplorasi
GROUP BY claim_status
ORDER BY jumlah_video DESC;

-- 2. Rata-rata view, like, dan share per claim_status
SELECT claim_status,
       ROUND(AVG(video_view_count))  AS rata_view,
       ROUND(AVG(video_like_count))  AS rata_like,
       ROUND(AVG(video_share_count)) AS rata_share
FROM workspace.tiktok_lake.tiktok_raw_eksplorasi
WHERE claim_status IS NOT NULL
GROUP BY claim_status;

-- 3. Sebaran author_ban_status terhadap claim_status
SELECT author_ban_status,
       COUNT_IF(claim_status = 'claim')   AS jumlah_claim,
       COUNT_IF(claim_status = 'opinion') AS jumlah_opinion,
       ROUND(100 * COUNT_IF(claim_status = 'claim') / COUNT(claim_status), 1) AS persen_claim
FROM workspace.tiktok_lake.tiktok_raw_eksplorasi
WHERE claim_status IS NOT NULL
GROUP BY author_ban_status
ORDER BY persen_claim DESC;
