create table public.llm_logs (
  id uuid not null default gen_random_uuid (),
  trace_id text null,
  user_id text null,
  model_name text not null,
  provider text null,
  base_url text null,
  input_params jsonb null,
  output_result jsonb null,
  total_tokens integer null,
  latency_ms integer null,
  status text null default 'success'::text,
  error_message text null,
  created_at timestamp with time zone null default now(),
  updated_at timestamp with time zone null default now(),
  constraint llm_logs_pkey primary key (id)
) TABLESPACE pg_default;

create index IF not exists idx_llm_logs_trace_id on public.llm_logs using btree (trace_id) TABLESPACE pg_default;

create index IF not exists idx_llm_logs_created_at on public.llm_logs using btree (created_at) TABLESPACE pg_default;

create index IF not exists idx_llm_logs_model_name on public.llm_logs using btree (model_name) TABLESPACE pg_default;

create index IF not exists idx_llm_logs_status on public.llm_logs using btree (status) TABLESPACE pg_default;