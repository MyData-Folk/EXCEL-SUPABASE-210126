create table if not exists public.disponibilites (
  id bigserial primary key,
  hotel_id uuid not null references public.hotels(id),
  date date not null,
  type_de_chambre text not null,
  disponibilites numeric null,
  ferme_a_la_vente text null
);

create table if not exists public.planning_tarifs (
  id bigserial primary key,
  hotel_id uuid not null references public.hotels(id),
  date date not null,
  type_de_chambre text not null,
  plan_tarifaire text null,
  tarif numeric null
);

create table if not exists public.booking_apercu (
  id bigserial primary key,
  hotel_id uuid not null references public.hotels(id),
  date_mise_a_jour timestamptz null,
  "Jour" text null,
  "Date" date null,
  "Votre hôtel le plus bas" numeric null,
  "Tarif le plus bas" numeric null,
  "médiane du compset" numeric null,
  "Classement des tarifs du compset" text null,
  "Demande du marché" numeric null,
  "Booking.com Classement" text null,
  "Jours fériés" text null,
  "Événements" text null
);

create table if not exists public.booking_tarifs (
  id bigserial primary key,
  hotel_id uuid not null references public.hotels(id),
  date_mise_a_jour timestamptz null,
  "Jour" text null,
  "Date" date null,
  "Demande du marché" numeric null
  -- Add dynamic competitor columns as NUMERIC.
);

create table if not exists public.booking_infos_tarifs (
  id bigserial primary key,
  hotel_id uuid not null references public.hotels(id),
  date_mise_a_jour timestamptz null,
  "Jour" text null,
  "Date" text null,
  "Demande du marché" text null
  -- Add dynamic competitor columns as TEXT.
);

create table if not exists public.booking_vs_hier (
  id bigserial primary key,
  hotel_id uuid not null references public.hotels(id),
  date_mise_a_jour timestamptz null,
  "Jour" text null,
  "Date" date null,
  "Demande du marché" numeric null,
  "vs.hier__Demande du marché" numeric null
  -- Add dynamic competitor columns + vs.hier__{Hotel} as NUMERIC.
);

create table if not exists public.booking_infos_vs_hier (
  id bigserial primary key,
  hotel_id uuid not null references public.hotels(id),
  date_mise_a_jour timestamptz null,
  "Jour" text null,
  "Date" text null,
  "Demande du marché" text null,
  "vs.hier__Demande du marché" text null
  -- Add dynamic competitor columns + vs.hier__{Hotel} as TEXT.
);

create table if not exists public.booking_vs_3j (
  id bigserial primary key,
  hotel_id uuid not null references public.hotels(id),
  date_mise_a_jour timestamptz null,
  "Jour" text null,
  "Date" date null,
  "Demande du marché" numeric null,
  "vs.3j__Demande du marché" numeric null
  -- Add dynamic competitor columns + vs.3j__{Hotel} as NUMERIC.
);

create table if not exists public.booking_infos_vs_3j (
  id bigserial primary key,
  hotel_id uuid not null references public.hotels(id),
  date_mise_a_jour timestamptz null,
  "Jour" text null,
  "Date" text null,
  "Demande du marché" text null,
  "vs.3j__Demande du marché" text null
  -- Add dynamic competitor columns + vs.3j__{Hotel} as TEXT.
);

create table if not exists public.booking_vs_7j (
  id bigserial primary key,
  hotel_id uuid not null references public.hotels(id),
  date_mise_a_jour timestamptz null,
  "Jour" text null,
  "Date" date null,
  "Demande du marché" numeric null,
  "vs.7j__Demande du marché" numeric null
  -- Add dynamic competitor columns + vs.7j__{Hotel} as NUMERIC.
);

create table if not exists public.booking_infos_vs_7j (
  id bigserial primary key,
  hotel_id uuid not null references public.hotels(id),
  date_mise_a_jour timestamptz null,
  "Jour" text null,
  "Date" text null,
  "Demande du marché" text null,
  "vs.7j__Demande du marché" text null
  -- Add dynamic competitor columns + vs.7j__{Hotel} as TEXT.
);

create table if not exists public.booking_export (
  id bigserial primary key,
  hotel_id uuid not null references public.hotels(id),
  "Date d'arrivée" date null,
  "Date de départ" date null,
  "Date d'achat" date null,
  "Heure d'achat" time null,
  "Dernière modification" date null,
  "Heure de dernière modification" time null,
  "Date d'annulation" date null,
  "Heure d'annulation" time null,
  "Hôtel (ID)" integer null,
  "Nuits" integer null,
  "Chambres" integer null,
  "Adultes" integer null,
  "Enfants" integer null,
  "Bébés" integer null,
  "Garantie" integer null,
  "Partenaire de distribution (ID)" integer null,
  "Montant total" numeric null,
  "Montant du panier" numeric null
  -- Add all remaining columns from BookingExport as TEXT.
);

create table if not exists public.events_calendar (
  id bigserial primary key,
  hotel_id uuid not null references public.hotels(id),
  "Événement" text null,
  "Début" date null,
  "Fin" date null,
  "Indice impact attendu sur la demande /10" numeric null,
  "Multiplicateur" numeric null,
  "Pourquoi cet indice" text null,
  "Conseils yield" text null
);
