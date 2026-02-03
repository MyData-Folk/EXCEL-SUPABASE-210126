-- =========================================
-- Extensions
-- =========================================
create extension if not exists pgcrypto;

-- =========================================
-- updated_at helper
-- =========================================
create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

-- =========================================
-- HOTELS (standard: hotel_id / hotel_name)
-- =========================================
create table if not exists public.hotels (
  id uuid not null default gen_random_uuid(),
  hotel_id text not null,
  hotel_name text not null,
  city text null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint hotels_pkey primary key (id),
  constraint hotels_hotel_id_key unique (hotel_id)
);

create index if not exists idx_hotels_hotel_id on public.hotels (hotel_id);

drop trigger if exists trg_hotels_updated_at on public.hotels;
create trigger trg_hotels_updated_at
before update on public.hotels
for each row execute function public.set_updated_at();

-- =========================================
-- RMS / UI PARAM: competitor set
-- =========================================
create table if not exists public.hotel_competitor_set (
  id bigserial primary key,
  hotel_fk uuid not null references public.hotels(id) on delete cascade,
  source text not null default 'booking',
  competitor_name text not null,
  is_self boolean not null default false,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint hotel_competitor_set_unique unique (hotel_fk, source, competitor_name)
);

create index if not exists idx_hcs_hotel_fk on public.hotel_competitor_set (hotel_fk, source);

drop trigger if exists trg_hcs_updated_at on public.hotel_competitor_set;
create trigger trg_hcs_updated_at
before update on public.hotel_competitor_set
for each row execute function public.set_updated_at();

-- =========================================
-- IMPORT RUNS
-- =========================================
create table if not exists public.import_runs (
  id bigserial primary key,
  hotel_fk uuid not null references public.hotels(id) on delete cascade,
  source_file_name text not null,
  template text not null,
  started_at timestamptz not null default now(),
  finished_at timestamptz null,
  status text not null default 'running',
  error_message text null,
  meta jsonb not null default '{}'::jsonb
);

create index if not exists idx_import_runs_hotel on public.import_runs (hotel_fk, started_at desc);

-- =========================================
-- PLANNING
-- =========================================
create table if not exists public.planning_tarifs (
  id bigserial primary key,
  hotel_fk uuid not null references public.hotels(id) on delete cascade,
  date date not null,
  type_de_chambre text not null,
  plan_tarifaire text not null,
  tarif numeric null
);

create index if not exists idx_planning_tarifs_hotel_date on public.planning_tarifs (hotel_fk, date);
create index if not exists idx_planning_tarifs_hotel_room_date on public.planning_tarifs (hotel_fk, type_de_chambre, date);

create table if not exists public.disponibilites (
  id bigserial primary key,
  hotel_fk uuid not null references public.hotels(id) on delete cascade,
  date date not null,
  type_de_chambre text not null,
  disponibilites numeric null,
  ferme_a_la_vente text null
);

create index if not exists idx_dispo_hotel_date on public.disponibilites (hotel_fk, date);
create index if not exists idx_dispo_hotel_room_date on public.disponibilites (hotel_fk, type_de_chambre, date);

-- =========================================
-- BOOKING.COM LOWEST LOS: Aperçu
-- =========================================
create table if not exists public.booking_apercu (
  id bigserial primary key,
  hotel_fk uuid not null references public.hotels(id) on delete cascade,
  date_mise_a_jour timestamptz null,
  "Jour" text null,
  "Date" date null,
  "Votre hôtel le plus bas" numeric null,
  "Tarif le plus bas" numeric null,
  "médiane du compset" numeric null,
  "Tarif le plus bas, médiane du compset" numeric null,
  "Classement des tarifs du compset" text null,
  "Demande du marché" numeric null,
  "Booking.com Classement" text null,
  "Jours fériés" text null,
  "Événements" text null,
  raw_row jsonb not null default '{}'::jsonb
);

create index if not exists idx_booking_apercu_hotel_date on public.booking_apercu (hotel_fk, "Date");

-- =========================================
-- BOOKING.COM LOWEST LOS: Tarifs
-- =========================================
create table if not exists public.booking_tarifs_market (
  id bigserial primary key,
  hotel_fk uuid not null references public.hotels(id) on delete cascade,
  date_mise_a_jour timestamptz null,
  "Jour" text null,
  "Date" date not null,
  "Demande du marché" numeric null,
  raw_row jsonb not null default '{}'::jsonb
);

create index if not exists idx_booking_tarifs_market_hotel_date on public.booking_tarifs_market (hotel_fk, "Date");

create table if not exists public.booking_tarifs_competitors (
  id bigserial primary key,
  hotel_fk uuid not null references public.hotels(id) on delete cascade,
  date_mise_a_jour timestamptz null,
  "Jour" text null,
  "Date" date not null,
  competitor_name text not null,
  price numeric null,
  raw_price text null
);

create index if not exists idx_booking_tarifs_comp_hotel_date on public.booking_tarifs_competitors (hotel_fk, "Date");
create index if not exists idx_booking_tarifs_comp_hotel_comp on public.booking_tarifs_competitors (hotel_fk, competitor_name);

-- =========================================
-- BOOKING.COM LOWEST LOS: vs.*
-- =========================================
create table if not exists public.booking_vs_market (
  id bigserial primary key,
  hotel_fk uuid not null references public.hotels(id) on delete cascade,
  date_mise_a_jour timestamptz null,
  horizon text not null,
  "Jour" text null,
  "Date" date not null,
  "Demande du marché" numeric null,
  delta_demande numeric null,
  raw_demande text null,
  raw_delta_demande text null,
  raw_row jsonb not null default '{}'::jsonb,
  constraint booking_vs_market_horizon_chk check (horizon in ('hier','3j','7j'))
);

create index if not exists idx_booking_vs_market_hotel_date on public.booking_vs_market (hotel_fk, horizon, "Date");

create table if not exists public.booking_vs_competitors (
  id bigserial primary key,
  hotel_fk uuid not null references public.hotels(id) on delete cascade,
  date_mise_a_jour timestamptz null,
  horizon text not null,
  "Jour" text null,
  "Date" date not null,
  competitor_name text not null,
  price numeric null,
  delta numeric null,
  raw_price text null,
  raw_delta text null,
  constraint booking_vs_comp_horizon_chk check (horizon in ('hier','3j','7j'))
);

create index if not exists idx_booking_vs_comp_hotel_date on public.booking_vs_competitors (hotel_fk, horizon, "Date");
create index if not exists idx_booking_vs_comp_hotel_comp on public.booking_vs_competitors (hotel_fk, horizon, competitor_name);

-- =========================================
-- BOOKING EXPORT
-- =========================================
create table if not exists public.booking_export (
  id bigserial primary key,
  hotel_fk uuid not null references public.hotels(id) on delete cascade,
  "Etat" text null,
  "Référence" text null,
  "Date d'achat" date null,
  "Heure d'achat" time null,
  "Dernière modification" date null,
  "Heure de dernière modification" time null,
  "Date d'annulation" date null,
  "Heure d'annulation" time null,
  "Hôtel" text null,
  "Hôtel (ID)" integer null,
  "Titre" text null,
  "Prénom" text null,
  "Nom" text null,
  "E-Mail" text null,
  "Date d'arrivée" date null,
  "Date de départ" date null,
  "Nuits" integer null,
  "Chambres" integer null,
  "Adultes" integer null,
  "Enfants" integer null,
  "Bébés" integer null,
  "Type de chambre" text null,
  "Tarif" text null,
  "Détail du panier" text null,
  "Tarifs multiples" text null,
  "Demande de réservation" text null,
  "Code promo" text null,
  "Visibilité du code promo" text null,
  "Montant total" numeric null,
  "Montant du panier" numeric null,
  "Montant restant" numeric null,
  "Garantie" integer null,
  "Monnaie" text null,
  "Mode de paiement" text null,
  "Plateforme" text null,
  "Assurance annulation" text null,
  "Etat du paiement" text null,
  "Produit" text null,
  "Type d’origine" text null,
  "Origine" text null,
  "Partenaire de distribution" text null,
  "Partenaire de distribution (ID)" integer null,
  "Référence partenaire" text null,
  "Evaluation client" text null,
  "Langue" text null,
  "Pays" text null,
  "Code postal client" text null,
  "Téléphone" text null,
  "Moteur de réservation" text null,
  "Referrer" text null,
  "Origin" text null,
  "Commentaire client (BE seulement)" text null,
  "Compte société" text null,
  "Utilisateur compte société" text null,
  "Société" text null,
  "Opt-in hôtel accepté" text null,
  "Opt-in partenaires accepté" text null,
  "Motif de l'annulation" text null,
  "Facturation de l'annulation" text null
);

create index if not exists idx_booking_export_hotel_arrivee on public.booking_export (hotel_fk, "Date d'arrivée");

-- =========================================
-- EVENTS
-- =========================================
create table if not exists public.events_calendar (
  id bigserial primary key,
  hotel_fk uuid not null references public.hotels(id) on delete cascade,
  "Événement" text null,
  "Début" date null,
  "Fin" date null,
  "Indice impact attendu sur la demande /10" numeric null,
  "Multiplicateur" numeric null,
  "Pourquoi cet indice" text null,
  "Conseils yield" text null
);

create index if not exists idx_events_calendar_hotel_debut on public.events_calendar (hotel_fk, "Début");

-- =========================================
-- VIEWS
-- =========================================
create or replace view public.vw_booking_tarifs_wide_json as
select
  m.hotel_fk,
  m.date_mise_a_jour,
  m."Jour",
  m."Date",
  m."Demande du marché",
  coalesce(
    jsonb_object_agg(c.competitor_name, c.price) filter (where c.competitor_name is not null),
    '{}'::jsonb
  ) as competitor_prices
from public.booking_tarifs_market m
left join public.booking_tarifs_competitors c
  on c.hotel_fk = m.hotel_fk
 and c."Date" = m."Date"
group by m.hotel_fk, m.date_mise_a_jour, m."Jour", m."Date", m."Demande du marché";

create or replace view public.vw_booking_vs_wide_json as
select
  v.hotel_fk,
  v.horizon,
  v.date_mise_a_jour,
  v."Jour",
  v."Date",
  v."Demande du marché",
  v.delta_demande,
  coalesce(
    jsonb_object_agg(
      c.competitor_name,
      jsonb_build_object('price', c.price, 'delta', c.delta)
    ) filter (where c.competitor_name is not null),
    '{}'::jsonb
  ) as competitor_prices
from public.booking_vs_market v
left join public.booking_vs_competitors c
  on c.hotel_fk = v.hotel_fk
 and c.horizon = v.horizon
 and c."Date" = v."Date"
group by v.hotel_fk, v.horizon, v.date_mise_a_jour, v."Jour", v."Date", v."Demande du marché", v.delta_demande;
