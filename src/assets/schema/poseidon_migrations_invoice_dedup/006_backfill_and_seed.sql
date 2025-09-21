begin;

update schema_poseidon_tst_16_adm.invoice_line l
   set content_hash = schema_poseidon_tst_16_adm._hash_invoice_line(l)
 where content_hash is null;

insert into schema_poseidon_tst_16_adm.invoice_line_version
(
  id, creation_user, creation_date, update_user, update_date,
  reference, batch_reference, source_type, flow_reference, output_reference,
  provider_org_unit, extraction_timestamp, business_timestamp,
  billing_code_provider, billing_code_consumer, production_resource,
  quantity, unit_cost, total_cost, cost_component_reference,
  cost_component_name, cost_component_type, description, technical_description,
  finance_comment, environment, anomalies, metadata, "function",
  invoicing_cycle_id, billing_item_type, line_version_no, is_active,
  billing_code_id, billing_code_description, customer_id, customer_name,
  product_name, product_group, product_domain, manual_modified_by_user_id,
  justification, processing_message, product_id, product_reference,
  valid_from, valid_to, is_current, versioning_comment, content_hash
)
select
  l.id, l.creation_user, l.creation_date, l.update_user, l.update_date,
  l.reference, l.batch_reference, l.source_type, l.flow_reference, l.output_reference,
  l.provider_org_unit, l.extraction_timestamp, l.business_timestamp,
  l.billing_code_provider, l.billing_code_consumer, l.production_resource,
  l.quantity, l.unit_cost, l.total_cost, l.cost_component_reference,
  l.cost_component_name, l.cost_component_type, l.description, l.technical_description,
  l.finance_comment, l.environment, l.anomalies, l.metadata, l."function",
  l.invoicing_cycle_id, l.billing_item_type,
  coalesce(l.line_version_no, 1) as line_version_no,
  l.is_active, l.billing_code_id, l.billing_code_description, l.customer_id, l.customer_name,
  l.product_name, l.product_group, l.product_domain, l.manual_modified_by_user_id,
  l.justification, l.processing_message, l.product_id, l.product_reference,
  l.valid_from, l.valid_to, l.is_current, l.versioning_comment, l.content_hash
from schema_poseidon_tst_16_adm.invoice_line l
on conflict (reference, content_hash) do nothing;

insert into schema_poseidon_tst_16_adm.output_invoice_line (output_reference, line_reference, line_version_no)
select l.output_reference, l.reference, coalesce(l.line_version_no, 1)
from   schema_poseidon_tst_16_adm.invoice_line l
where  l.output_reference is not null
on conflict (output_reference, line_reference) do nothing;

commit;
