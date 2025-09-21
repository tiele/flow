begin;

create table if not exists schema_poseidon_tst_16_adm.event_log (
  id                bigserial primary key,
  ts                timestamptz not null default now(),
  event_type        text not null,
  actor             text,
  source_type       text,
  flow_reference    text,
  output_reference  text,
  batch_reference   text,
  usage_reference   text,
  details           jsonb
);

create index if not exists idx_event_log_ts on schema_poseidon_tst_16_adm.event_log(ts);
create index if not exists idx_event_log_type on schema_poseidon_tst_16_adm.event_log(event_type);
create index if not exists idx_event_log_output on schema_poseidon_tst_16_adm.event_log(output_reference);
create index if not exists idx_event_log_batch on schema_poseidon_tst_16_adm.event_log(batch_reference);

commit;
