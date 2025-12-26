create table public.whop_posts (
  id text not null,
  feed_id text not null,
  user_id text null,
  content text null,
  rich_content jsonb null,
  reply_to_post_id text null,
  mentioned_user_ids text[] null,
  attachments jsonb null default '[]'::jsonb,
  link_embeds jsonb null default '[]'::jsonb,
  gifs jsonb null default '[]'::jsonb,
  reaction_counts jsonb null default '[]'::jsonb,
  view_count integer null default 0,
  is_pinned boolean null default false,
  is_edited boolean null default false,
  is_deleted boolean null default false,
  posted_at timestamp with time zone not null,
  edited_at timestamp with time zone null,
  crawled_at timestamp with time zone null default now(),
  constraint whop_posts_pkey primary key (id),
  constraint whop_posts_user_id_fkey foreign KEY (user_id) references whop_users (id)
) TABLESPACE pg_default;

create index IF not exists idx_whop_posts_feed_id on public.whop_posts using btree (feed_id) TABLESPACE pg_default;

create index IF not exists idx_whop_posts_user_id on public.whop_posts using btree (user_id) TABLESPACE pg_default;

create index IF not exists idx_whop_posts_posted_at on public.whop_posts using btree (posted_at desc) TABLESPACE pg_default;