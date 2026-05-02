-- ═══════════════════════════════════════════════════════════
--  lovable_setup.sql
--  Execute no SQL Editor do Lovable/Supabase APÓS criar o projeto.
--  Lovable → Settings → Database → SQL Editor
-- ═══════════════════════════════════════════════════════════

-- 1. UNIQUE constraint para upsert via id_externo
--    (necessário para o Python fazer insert-or-update corretamente)
ALTER TABLE contratos
  ADD CONSTRAINT contratos_id_externo_unique UNIQUE (id_externo);

-- 2. Índices para performance (filtragem e ordenação do dashboard)
CREATE INDEX IF NOT EXISTS idx_contratos_fonte
  ON contratos (fonte);

CREATE INDEX IF NOT EXISTS idx_contratos_kanban_etapa
  ON contratos (kanban_etapa);

CREATE INDEX IF NOT EXISTS idx_contratos_data_prevista
  ON contratos (data_prevista_entrega);

CREATE INDEX IF NOT EXISTS idx_contratos_pendentes
  ON contratos (data_entrega_definitiva)
  WHERE data_entrega_definitiva IS NULL;

CREATE INDEX IF NOT EXISTS idx_historico_contrato_id
  ON historico_status (contrato_id);

CREATE INDEX IF NOT EXISTS idx_sync_log_fonte
  ON sync_log (fonte, iniciado_em DESC);

-- 3. View: contratos pendentes com dias calculados (facilita consultas do Lovable)
CREATE OR REPLACE VIEW vw_contratos_pendentes AS
SELECT
  c.*,
  (c.data_prevista_entrega::date - CURRENT_DATE) AS dias_para_entrega,
  CASE
    WHEN (c.data_prevista_entrega::date - CURRENT_DATE) < 0 THEN true
    ELSE false
  END AS atrasado,
  CASE
    WHEN (c.data_prevista_entrega::date - CURRENT_DATE) < 0  THEN 'atrasado'
    WHEN (c.data_prevista_entrega::date - CURRENT_DATE) <= 5 THEN 'critico'
    WHEN (c.data_prevista_entrega::date - CURRENT_DATE) <= 15 THEN 'urgente'
    ELSE 'normal'
  END AS prioridade
FROM contratos c
WHERE c.data_entrega_definitiva IS NULL
ORDER BY c.data_prevista_entrega ASC NULLS LAST;

-- 4. View: estatísticas do dashboard (os 4 cards)
CREATE OR REPLACE VIEW vw_stats_dashboard AS
SELECT
  COUNT(*) FILTER (WHERE data_entrega_definitiva IS NULL) AS total_pendentes,
  COUNT(*) FILTER (
    WHERE data_entrega_definitiva IS NULL
      AND data_prevista_entrega::date < CURRENT_DATE
  ) AS atrasados,
  COUNT(*) FILTER (
    WHERE data_entrega_definitiva IS NULL
      AND data_prevista_entrega::date BETWEEN CURRENT_DATE AND CURRENT_DATE + 5
  ) AS criticos,
  COUNT(*) FILTER (
    WHERE data_entrega_definitiva::date = CURRENT_DATE
  ) AS entregues_hoje
FROM contratos;

-- 5. View: última sync por fonte
CREATE OR REPLACE VIEW vw_ultima_sync AS
SELECT
  fonte,
  MAX(iniciado_em) AS ultima_sync,
  (
    SELECT status FROM sync_log s2
    WHERE s2.fonte = s1.fonte
    ORDER BY iniciado_em DESC LIMIT 1
  ) AS ultimo_status
FROM sync_log s1
GROUP BY fonte;

-- 6. RLS — Acesso apenas para usuários autenticados
ALTER TABLE contratos      ENABLE ROW LEVEL SECURITY;
ALTER TABLE historico_status ENABLE ROW LEVEL SECURITY;
ALTER TABLE sync_log       ENABLE ROW LEVEL SECURITY;

-- Leitura e escrita para usuários autenticados
CREATE POLICY "auth_contratos" ON contratos
  FOR ALL TO authenticated USING (true) WITH CHECK (true);

CREATE POLICY "auth_historico" ON historico_status
  FOR ALL TO authenticated USING (true) WITH CHECK (true);

-- sync_log: qualquer autenticado pode inserir (Python usa service_role, bypassa)
CREATE POLICY "auth_sync_log" ON sync_log
  FOR ALL TO authenticated USING (true) WITH CHECK (true);

-- ═══════════════════════════════════════════════════════════
-- Pronto! Após executar, rode no terminal:
--   python sync_lovable.py --teste   (verifica conexão)
--   python sync_lovable.py --inicial (importa dados existentes)
-- ═══════════════════════════════════════════════════════════
