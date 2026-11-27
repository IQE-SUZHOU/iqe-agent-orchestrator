-- CI Triage Agent Pipeline：编排层状态库（幂等，可重复执行）
create table if not exists cases (
  case_id       text primary key,
  run_id        text not null,
  scan_id       text,
  source        text not null default 'mock',
  job           text not null,
  build         integer,
  signal        text not null check (signal in ('none','error')),
  signature     text,
  status        text not null,
  workflow_key  text,
  plan_id       text,
  plan_source   text,
  priority      integer not null default 1,
  scenario_hint text,
  progress      jsonb not null default '{}'::jsonb,
  payload       jsonb not null default '{}'::jsonb,
  blocked_reason text,
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now()
);
create index if not exists idx_cases_status on cases(status);
create index if not exists idx_cases_created on cases(created_at desc);

alter table cases add column if not exists case_report jsonb;

create table if not exists scans (
  scan_id     text primary key,
  run_id      text not null,
  scenario    text,
  started_at  timestamptz not null default now(),
  finished_at timestamptz,
  status      text not null default 'running',
  counts      jsonb not null default '{}'::jsonb,
  error       jsonb
);

create table if not exists stage_runs (
  id              bigserial primary key,
  case_id         text references cases(case_id) on delete cascade,
  run_id          text not null,
  scan_id         text,
  stage           text not null,
  step_id         text not null,
  mode            text,
  attempt         integer not null default 1,
  status          text not null default 'dispatched',
  idempotency_key text not null unique,
  started_at      timestamptz not null default now(),
  finished_at     timestamptz,
  latency_ms      integer,
  output          jsonb,
  error           jsonb
);
create index if not exists idx_stage_runs_case on stage_runs(case_id, started_at);
create index if not exists idx_stage_runs_stage on stage_runs(stage, started_at desc);

create table if not exists evidence (
  id         bigserial primary key,
  case_id    text,
  scan_id    text,
  stage      text not null,
  kind       text not null,
  uri        text,
  meta       jsonb not null default '{}'::jsonb,
  body       text,
  created_at timestamptz not null default now()
);
create index if not exists idx_evidence_case on evidence(case_id, id);
create index if not exists idx_evidence_scan on evidence(scan_id, id);

create table if not exists tickets (
  case_id        text not null,
  backend        text not null,
  external_key   text,
  url            text,
  ticket_status  text,
  duplicate_of   text,
  ticket_priority text,
  assignee_team  text,
  payload        jsonb not null default '{}'::jsonb,
  created_at     timestamptz not null default now()
);
create unique index if not exists uniq_tickets_case_backend on tickets(case_id, backend);

create table if not exists reviews (
  id             bigserial primary key,
  case_id        text not null,
  verdict        text not null,
  reason_code    text,
  requested_info jsonb not null default '[]'::jsonb,
  checks         jsonb not null default '[]'::jsonb,
  quality        jsonb not null default '{}'::jsonb,
  loop_seq       integer not null default 1,
  created_at     timestamptz not null default now()
);
create index if not exists idx_reviews_case on reviews(case_id, loop_seq);

create table if not exists daily_reports (
  day         date primary key,
  report_md   text not null,
  metrics     jsonb not null default '{}'::jsonb,
  cases       jsonb not null default '[]'::jsonb,
  published_at timestamptz not null default now()
);

create table if not exists msg_dedup (
  dedup_key  text primary key,
  stage      text not null,
  consumed_by text,
  created_at timestamptz not null default now()
);
create index if not exists idx_msg_dedup_created on msg_dedup(created_at);

create table if not exists workflows (
  key        text primary key,
  title      text,
  enabled    boolean not null default true,
  priority   integer not null default 0,
  match      jsonb not null default '{}'::jsonb,
  steps      jsonb not null default '[]'::jsonb,
  source     text,
  version    integer not null default 1,
  updated_at timestamptz not null default now()
);

create table if not exists plans (
  plan_id      text primary key,
  case_id      text,
  source       text not null,
  workflow_key text,
  degraded     boolean not null default false,
  file_path    text,
  yaml_sha256  text,
  validator    text,
  steps        jsonb not null default '[]'::jsonb,
  rationale    text,
  model        text,
  created_at   timestamptz not null default now()
);

create table if not exists dlq (
  id              bigserial primary key,
  stage           text,
  idempotency_key text,
  case_id         text,
  reason          text,
  original_routing_key text,
  redelivered     boolean not null default false,
  body            text,
  created_at      timestamptz not null default now()
);

-- LangGraph checkpoint 表由 AsyncPostgresSaver.setup() 自动创建（langgraph_ai_*）

-- ---------------------------------------------------------------- 12 步流水线（4a / 5a / 5b / 6c / 闸门）
-- reviews 增加 gate 列：同一 Case 会依次过 gate1 / gate2 / gate3。
alter table reviews add column if not exists gate text;

create table if not exists remediations (
  case_id      text primary key,
  change_type  text,
  target       text,
  fixed        boolean not null default false,
  work_item_key text,
  risk         text,
  diff_summary text,
  payload      jsonb not null default '{}'::jsonb,
  created_at   timestamptz not null default now()
);

create table if not exists verifications (
  case_id       text not null,
  mode          text not null,          -- evidence(4b) | verify(5a)
  reproducible  text,
  fix_verified  boolean,
  attempts      integer not null default 0,
  payload       jsonb not null default '{}'::jsonb,
  created_at    timestamptz not null default now(),
  primary key (case_id, mode)
);

create table if not exists regressions (
  case_id      text not null,
  mode         text not null,           -- check(5b) | bisect(6c)
  is_regression boolean,
  guilty_commit text,
  guilty_pr    text,
  confidence   numeric,
  payload      jsonb not null default '{}'::jsonb,
  created_at   timestamptz not null default now(),
  primary key (case_id, mode)
);

create table if not exists kb_entries (
  id           bigserial primary key,
  kind         text not null,
  component    text not null,
  signature    text,
  summary      text not null,
  case_id      text,
  created_at   timestamptz not null default now()
);
create index if not exists idx_kb_entries_lookup on kb_entries(component, signature);
