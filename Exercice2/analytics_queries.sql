
-- Table des événements
CREATE TABLE events (
    event_id SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    event_type VARCHAR(50), -- 'page_view', 'add_to_cart', 'purchase'
    timestamp TIMESTAMP DEFAULT NOW(),
    channel VARCHAR(50), -- 'organic', 'paid', 'email', 'social'
    revenue DECIMAL(10,2) DEFAULT 0,
    properties JSONB -- données additionnelles
);

-- Table des sessions
CREATE TABLE sessions (
    session_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    pages_viewed INT DEFAULT 0,
    converted BOOLEAN DEFAULT FALSE,
    channel VARCHAR(50)
);

-- Index pour la performance
CREATE INDEX idx_events_user ON events(user_id);
CREATE INDEX idx_events_type ON events(event_type);
CREATE INDEX idx_sessions_user ON sessions(user_id);
CREATE INDEX idx_events_timestamp ON events(timestamp);
CREATE INDEX idx_sessions_start ON sessions(start_time);

-- ============================================
-- INSERTION DE DONNÉES DE TEST
-- ============================================

-- Insertion d'événements de test
INSERT INTO events (user_id, event_type, channel, revenue, timestamp) VALUES
('user_001', 'page_view', 'organic', 0, '2025-01-15 10:00:00'),
('user_001', 'add_to_cart', 'organic', 0, '2025-01-15 10:05:00'),
('user_001', 'purchase', 'organic', 150.00, '2025-01-15 10:10:00'),
('user_002', 'page_view', 'paid', 0, '2025-01-15 11:00:00'),
('user_002', 'page_view', 'paid', 0, '2025-01-15 11:05:00'),
('user_003', 'page_view', 'email', 0, '2025-01-15 12:00:00'),
('user_003', 'add_to_cart', 'email', 0, '2025-01-15 12:10:00'),
('user_003', 'purchase', 'email', 250.00, '2025-01-15 12:20:00'),
('user_004', 'page_view', 'social', 0, '2025-01-15 13:00:00'),
('user_005', 'page_view', 'organic', 0, '2025-01-15 14:00:00'),
('user_005', 'purchase', 'organic', 75.00, '2025-01-15 14:15:00'),
('user_006', 'page_view', 'paid', 0, '2025-01-15 15:00:00'),
('user_006', 'add_to_cart', 'paid', 0, '2025-01-15 15:10:00'),
('user_006', 'purchase', 'paid', 320.00, '2025-01-15 15:20:00'),
('user_007', 'page_view', 'email', 0, '2025-01-15 16:00:00'),
('user_008', 'page_view', 'social', 0, '2025-01-15 17:00:00'),
('user_008', 'purchase', 'social', 180.00, '2025-01-15 17:30:00'),
('user_009', 'page_view', 'organic', 0, '2025-01-15 18:00:00'),
('user_010', 'page_view', 'paid', 0, '2025-01-15 19:00:00');

-- Insertion de sessions de test
INSERT INTO sessions (session_id, user_id, start_time, end_time, pages_viewed, converted, channel) VALUES
('sess_001', 'user_001', '2025-01-15 10:00:00', '2025-01-15 10:15:00', 5, TRUE, 'organic'),
('sess_002', 'user_002', '2025-01-15 11:00:00', '2025-01-15 11:10:00', 2, FALSE, 'paid'),
('sess_003', 'user_003', '2025-01-15 12:00:00', '2025-01-15 12:25:00', 7, TRUE, 'email'),
('sess_004', 'user_004', '2025-01-15 13:00:00', '2025-01-15 13:05:00', 1, FALSE, 'social'),
('sess_005', 'user_005', '2025-01-15 14:00:00', '2025-01-15 14:20:00', 3, TRUE, 'organic'),
('sess_006', 'user_006', '2025-01-15 15:00:00', '2025-01-15 15:25:00', 6, TRUE, 'paid'),
('sess_007', 'user_007', '2025-01-15 16:00:00', '2025-01-15 16:05:00', 1, FALSE, 'email'),
('sess_008', 'user_008', '2025-01-15 17:00:00', '2025-01-15 17:35:00', 4, TRUE, 'social'),
('sess_009', 'user_009', '2025-01-15 18:00:00', '2025-01-15 18:08:00', 2, FALSE, 'organic'),
('sess_010', 'user_010', '2025-01-15 19:00:00', '2025-01-15 19:05:00', 1, FALSE, 'paid'),
('sess_011', 'user_006', '2025-01-15 15:00:00', '2025-01-15 15:30:00', 6, TRUE, 'paid'),
('sess_012', 'user_008', '2025-01-15 17:00:00', '2025-01-15 18:25:00', 4, TRUE, 'social'),
('sess_013', 'user_009', '2025-01-15 18:00:00', '2025-01-15 15:08:00', 2, FALSE, 'organic'),
('sess_014', 'user_010', '2025-01-15 19:00:00', '2025-01-15 19:05:00', 1, FALSE, 'paid');

-- Exercice 2.1.1 : Requêtes SQL Analytics 
-- ============================================
-- REQUÊTE 1 : TAUX DE CONVERSION PAR CANAL
-- ============================================

-- Version fournie dans le TP
SELECT 
    channel,
    COUNT(DISTINCT CASE WHEN converted THEN user_id END) * 100.0 / 
    COUNT(DISTINCT user_id) AS conversion_rate
FROM sessions
GROUP BY channel
ORDER BY conversion_rate DESC;

-- 9) Taux de conversion par canal (utilisateurs convertis / utilisateurs uniques par canal)
SELECT
  channel,
  (COUNT(DISTINCT user_id) FILTER (WHERE converted))::float
    / NULLIF(COUNT(DISTINCT user_id),0) * 100 AS conversion_rate_percent
FROM sessions
GROUP BY channel
ORDER BY conversion_rate_percent DESC;

-- ============================================
-- REQUÊTE 2 : REVENU MOYEN PAR UTILISATEUR (ARPU)
-- ============================================
-- ARPU = total revenue / nombre d'utilisateurs uniques
SELECT
  SUM(revenue)::numeric(12,2) / NULLIF(COUNT(DISTINCT user_id),0) AS arpu
FROM events;

-- ============================================
-- REQUÊTE 3 : TOP 5 DES HEURES AVEC LE MEILLEUR TAUX DE CONVERSION
-- ============================================

-- On agrège par heure (0-23) et on calcule le taux de conversion (sessions converties / total sessions)
SELECT
  extract(hour from start_time)::int AS hour_of_day,
  SUM(CASE WHEN converted THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*),0) AS conversion_rate
FROM sessions
GROUP BY hour_of_day
ORDER BY conversion_rate DESC
LIMIT 5;


-- ============================================
--  REQUÊTE 4 : rétention des utilisateurs par mois d'inscription
-- ============================================

-- On considère le premier événement (ou signup) comme mois d'inscription (cohort_month).
-- Puis on calcule le nombre d'utilisateurs actifs par mois suivant le cohort_month.
WITH first_event AS (
  SELECT user_id, date_trunc('month', MIN(timestamp))::date AS cohort_month
  FROM events
  GROUP BY user_id
),
activity AS (
  SELECT user_id, date_trunc('month', timestamp)::date AS activity_month
  FROM events
  GROUP BY user_id, date_trunc('month', timestamp)
),
cohort_activity AS (
  SELECT
    f.user_id,
    f.cohort_month,
    a.activity_month,
    DATE_PART('month', age(a.activity_month, f.cohort_month))::int AS month_number
  FROM first_event f
  JOIN activity a ON f.user_id = a.user_id
)
SELECT
  cohort_month,
  month_number,
  COUNT(DISTINCT user_id) AS users_active
FROM cohort_activity
GROUP BY cohort_month, month_number
ORDER BY cohort_month, month_number;


-- Exercice 2.1.2 : Pipeline ETL avec Python 

-- ============================================
-- Création des tables pour le pipline ETL
-- ============================================

CREATE TABLE IF NOT EXISTS daily_metrics
(
    id integer NOT NULL DEFAULT nextval('daily_metrics_id_seq'::regclass),
    date date NOT NULL,
    visits integer DEFAULT 0,
    unique_visitors integer DEFAULT 0,
    actions integer DEFAULT 0,
    conversions integer DEFAULT 0,
    conversion_rate numeric(5,2) DEFAULT 0,
    bounce_rate numeric(5,2) DEFAULT 0,
    avg_time_on_site integer DEFAULT 0,
    pages_per_visit numeric(5,2) DEFAULT 0,
    CONSTRAINT daily_metrics_pkey PRIMARY KEY (id),
    CONSTRAINT daily_metrics_date_key UNIQUE (date)
)

CREATE TABLE IF NOT EXISTS channel_metrics
(
    id integer NOT NULL DEFAULT nextval('channel_metrics_id_seq'::regclass),
    date date NOT NULL,
    channel character varying(50) COLLATE pg_catalog."default" NOT NULL,
    visits integer DEFAULT 0,
    conversions integer DEFAULT 0,
    revenue numeric(10,2) DEFAULT 0,
    CONSTRAINT channel_metrics_pkey PRIMARY KEY (id),
    CONSTRAINT channel_metrics_date_channel_key UNIQUE (date, channel)
)



