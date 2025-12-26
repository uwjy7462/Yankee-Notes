create table public.whop_summaries (
  id uuid not null default gen_random_uuid (),
  title text null,
  description text null,
  content text null,
  knowledge_content text null,
  timeline_content text null,
  raw_chat_text text null,
  model_name text null,
  created_at timestamp with time zone null default now(),
  updated_at timestamp with time zone null default now(),
  tags text[] null,
  constraint whop_summaries_pkey primary key (id)
) TABLESPACE pg_default;

create index IF not exists idx_whop_summaries_created_at on public.whop_summaries using btree (created_at desc) TABLESPACE pg_default;