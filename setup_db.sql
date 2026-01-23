-- ============================================================================
-- Supabase Auto-Importer (RMS Sync) v2.0
-- Script d'initialisation complet pour Supabase
-- ============================================================================

-- 1. NETTOYAGE (Optionnel : à n'utiliser que si vous voulez repartir de zéro)
-- DROP VIEW IF EXISTS public.v_template_summary;
-- DROP FUNCTION IF EXISTS public.execute_sql(text);
-- DROP FUNCTION IF EXISTS public.get_public_tables();
-- DROP FUNCTION IF EXISTS public.get_table_columns(text);
-- DROP TABLE IF EXISTS public.import_templates;
-- DROP TABLE IF EXISTS public.hotels;

-- ============================================================================
-- TABLE: hotels
-- Stocke les informations des hôtels gérés
-- ============================================================================
CREATE TABLE IF NOT EXISTS public.hotels (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    hotel_id TEXT NOT NULL UNIQUE,
    hotel_name TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- Activation de la sécurité (RLS)
ALTER TABLE public.hotels ENABLE ROW LEVEL SECURITY;

-- Politique : Autoriser toutes les opérations pour le rôle service_role (Admin)
DROP POLICY IF EXISTS "Allow all for service role" ON public.hotels;
CREATE POLICY "Allow all for service role" ON public.hotels
    FOR ALL USING (true) WITH CHECK (true);

-- ============================================================================
-- TABLE: import_templates
-- Stocke les configurations de mapping pour les imports récurrents
-- ============================================================================
CREATE TABLE IF NOT EXISTS public.import_templates (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    source_type TEXT CHECK (source_type IN ('csv', 'excel')),
    target_table TEXT NOT NULL,
    sheet_name TEXT,
    column_mapping JSONB NOT NULL DEFAULT '{}',
    column_types JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- Activation de la sécurité (RLS)
ALTER TABLE public.import_templates ENABLE ROW LEVEL SECURITY;

-- Politique : Autoriser toutes les opérations pour le rôle service_role (Admin)
-- Si vous utilisez l'application avec la clé service_role, elle aura accès à tout.
DROP POLICY IF EXISTS "Allow all for service role" ON public.import_templates;
CREATE POLICY "Allow all for service role" ON public.import_templates
    FOR ALL USING (true) WITH CHECK (true);

-- ============================================================================
-- TABLES DE RAPPORTS (D-EDGE, OTA, EVENTS)
-- Ces tables sont utilisées par le traitement automatique
-- ============================================================================

-- 1. D-EDGE PLANNING
CREATE TABLE IF NOT EXISTS public."D-EDGE PLANNING TARIFS DISPO ET PLANS TARIFAIRES" (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
    hotel_id TEXT NOT NULL,
    "TYPE DE CHAMBRE" TEXT,
    "PLAN TARIFAIRE" TEXT,
    "LEFT FOR SALE" TEXT,
    "date" DATE,
    "TARIF / DISPO" TEXT
);

-- 2. D-EDGE RESERVATIONS (En cours & Historique)
-- Structure générique car dépend du fichier Excel
CREATE TABLE IF NOT EXISTS public."D-EDGE RÉSERVATIONS EN COURS" (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
    hotel_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS public."D-EDGE HISTORIQUE DES RÉSERVATIONS N-1" (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
    hotel_id TEXT NOT NULL
);

-- 3. OTA INSIGHT
CREATE TABLE IF NOT EXISTS public."OTA APERÇU" ( id UUID DEFAULT gen_random_uuid() PRIMARY KEY, created_at TIMESTAMP DEFAULT now(), hotel_id TEXT NOT NULL );
CREATE TABLE IF NOT EXISTS public."OTA TARIFS CONCURRENCE" ( id UUID DEFAULT gen_random_uuid() PRIMARY KEY, created_at TIMESTAMP DEFAULT now(), hotel_id TEXT NOT NULL );
CREATE TABLE IF NOT EXISTS public."OTA VS HIER" ( id UUID DEFAULT gen_random_uuid() PRIMARY KEY, created_at TIMESTAMP DEFAULT now(), hotel_id TEXT NOT NULL );
CREATE TABLE IF NOT EXISTS public."OTA VS 3 JOURS" ( id UUID DEFAULT gen_random_uuid() PRIMARY KEY, created_at TIMESTAMP DEFAULT now(), hotel_id TEXT NOT NULL );
CREATE TABLE IF NOT EXISTS public."OTA VS 7 JOURS" ( id UUID DEFAULT gen_random_uuid() PRIMARY KEY, created_at TIMESTAMP DEFAULT now(), hotel_id TEXT NOT NULL );

-- 4. SALONS & EVENTS
CREATE TABLE IF NOT EXISTS public."DATES SALONS ET ÉVÉNEMENTS" (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
    hotel_id TEXT NOT NULL
);

-- CRITIQUE : S'assurer que hotel_id existe si les tables existaient déjà
DO $$
DECLARE
    t text;
    tables text[] := ARRAY[
        'D-EDGE PLANNING TARIFS DISPO ET PLANS TARIFAIRES',
        'D-EDGE RÉSERVATIONS EN COURS',
        'D-EDGE HISTORIQUE DES RÉSERVATIONS N-1',
        'OTA APERÇU',
        'OTA TARIFS CONCURRENCE',
        'OTA VS HIER',
        'OTA VS 3 JOURS',
        'OTA VS 7 JOURS',
        'DATES SALONS ET ÉVÉNEMENTS'
    ];
BEGIN
    FOREACH t IN ARRAY tables LOOP
        BEGIN
            EXECUTE format('ALTER TABLE public.%I ADD COLUMN IF NOT EXISTS hotel_id TEXT', t);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Erreur lors de l''ajout de hotel_id pour %: %', t, SQLERRM;
        END;
        
        -- Ajouter les colonnes spécifiques manquantes pour D-EDGE PLANNING
        IF t = 'D-EDGE PLANNING TARIFS DISPO ET PLANS TARIFAIRES' THEN
            BEGIN
                EXECUTE 'ALTER TABLE public."D-EDGE PLANNING TARIFS DISPO ET PLANS TARIFAIRES" ADD COLUMN IF NOT EXISTS "LEFT FOR SALE" TEXT';
                EXECUTE 'ALTER TABLE public."D-EDGE PLANNING TARIFS DISPO ET PLANS TARIFAIRES" ADD COLUMN IF NOT EXISTS "TYPE DE CHAMBRE" TEXT';
                EXECUTE 'ALTER TABLE public."D-EDGE PLANNING TARIFS DISPO ET PLANS TARIFAIRES" ADD COLUMN IF NOT EXISTS "PLAN TARIFAIRE" TEXT';
                EXECUTE 'ALTER TABLE public."D-EDGE PLANNING TARIFS DISPO ET PLANS TARIFAIRES" ADD COLUMN IF NOT EXISTS "TARIF / DISPO" TEXT';
                 EXECUTE 'ALTER TABLE public."D-EDGE PLANNING TARIFS DISPO ET PLANS TARIFAIRES" ADD COLUMN IF NOT EXISTS "date" DATE';
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Erreur colonnes D-EDGE: %', SQLERRM;
            END;
        END IF;
        
        -- Ajouter la policy RLS pour chaque table report
        BEGIN
            EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', t);
            EXECUTE format('DROP POLICY IF EXISTS "Allow all" ON public.%I', t);
            EXECUTE format('CREATE POLICY "Allow all" ON public.%I FOR ALL USING (true) WITH CHECK (true)', t);
        EXCEPTION WHEN OTHERS THEN
            NULL;
        END;
    END LOOP;
END $$;


-- ============================================================================
-- FONCTION: execute_sql(sql_query text)
-- CRITIQUE : Permet à Python d'exécuter des commandes de structure (CREATE TABLE)
-- ============================================================================
CREATE OR REPLACE FUNCTION public.execute_sql(sql text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER -- Exécuté avec les privilèges de l'admin
AS $$
BEGIN
    EXECUTE sql;
END;
$$;

COMMENT ON FUNCTION public.execute_sql IS 'Permet l exécution de SQL dynamique (utilisé pour la création automatique de tables).';


-- ============================================================================
-- FONCTION: get_public_tables()
-- Liste les tables pour les proposer dans l interface (Etape 2)
-- ============================================================================
CREATE OR REPLACE FUNCTION public.get_public_tables()
RETURNS TABLE (
    table_name TEXT,
    table_type TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    RETURN QUERY
    SELECT
        t.table_name::TEXT,
        t.table_type::TEXT
    FROM information_schema.tables t
    WHERE t.table_schema = 'public'
    AND t.table_name NOT LIKE 'pg_%'
    AND t.table_name NOT LIKE 'sql_%'
    -- On exclut la table des templates elle-même de la liste d'import
    AND t.table_name != 'import_templates'
    ORDER BY t.table_name;
END;
$$;


-- ============================================================================
-- FONCTION: get_table_columns(t_name TEXT)
-- Retourne les colonnes et types d une table pour le mapping (Etape 3)
-- ============================================================================
CREATE OR REPLACE FUNCTION public.get_table_columns(t_name TEXT)
RETURNS TABLE (
    column_name TEXT,
    data_type TEXT,
    is_nullable TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.column_name::TEXT,
        c.data_type::TEXT,
        c.is_nullable::TEXT
    FROM information_schema.columns c
    WHERE c.table_schema = 'public'
    AND c.table_name = t_name
    -- On exclut les colonnes techniques générées par défaut
    AND c.column_name NOT IN ('id', 'created_at')
    ORDER BY c.ordinal_position;
END;
$$;


-- ============================================================================
-- VUE: v_template_summary
-- Résumé lisible des templates pour la barre latérale
-- ============================================================================
CREATE OR REPLACE VIEW public.v_template_summary AS
SELECT
    id,
    name,
    target_table,
    source_type,
    updated_at
FROM public.import_templates
ORDER BY updated_at DESC;


-- ============================================================================
-- INDEX & COMMENTAIRES
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_import_templates_target ON public.import_templates(target_table);

COMMENT ON TABLE public.import_templates IS 'Configuration ETL pour RMS Sync v2.0';