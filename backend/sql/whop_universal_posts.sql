create table public.whop_universal_posts (
  id text not null,
  title text null,
  content text null,
  rich_content jsonb null,
  feed_id text null,
  user_id text null,
  comment_count integer null default 0,
  view_count integer null default 0,
  is_pinned boolean null default false,
  reaction_counts jsonb null default '[]'::jsonb,
  attachments jsonb null default '[]'::jsonb,
  mentioned_user_ids text[] null,
  is_deleted boolean null default false,
  is_edited boolean null default false,
  posted_at timestamp with time zone not null,
  crawled_at timestamp with time zone null default now(),
  constraint whop_universal_posts_pkey primary key (id),
  constraint whop_universal_posts_user_id_fkey foreign KEY (user_id) references whop_users (id)
) TABLESPACE pg_default;

create index IF not exists idx_whop_universal_posts_feed_id on public.whop_universal_posts using btree (feed_id) TABLESPACE pg_default;
create index IF not exists idx_whop_universal_posts_user_id on public.whop_universal_posts using btree (user_id) TABLESPACE pg_default;
create index IF not exists idx_whop_universal_posts_posted_at on public.whop_universal_posts using btree (posted_at desc) TABLESPACE pg_default;
