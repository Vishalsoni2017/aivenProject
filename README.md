CREATE TABLE IF NOT EXISTS incidents (
  id BIGSERIAL PRIMARY KEY,
  created_at timestamptz DEFAULT now(),
  service_name text,
  issue text,
  action text,
  status text,
  details jsonb,
  raw_payload text,
  solution text
);
