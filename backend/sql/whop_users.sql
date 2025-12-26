create table public.whop_users (
  id text not null,
  username text null,
  display_name text null,
  avatar_url text null,
  roles text[] null,
  last_seen_at timestamp with time zone null,
  created_at timestamp with time zone null default now(),
  updated_at timestamp with time zone null default now(),
  constraint whop_users_pkey primary key (id)
) TABLESPACE pg_default;