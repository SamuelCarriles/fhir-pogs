CREATE TABLE fhir_search_functions (
  search_type TEXT NOT NULL,
  modifier TEXT NOT NULL,
  handler_function TEXT NOT NULL,
  description TEXT,
  PRIMARY KEY(search_type, modifier)
);
--;;
INSERT INTO fhir_search_functions (search_type, modifier, handler_function, description) VALUES
('token', 'base', 'token_search_base', 'FHIR token search without modifiers'),
('token', 'text', 'token_search_text', 'Search on the text/display of the token'),
('token', 'not', 'token_search_not', 'Reverse the search (exclude matches)'),
('token', 'of-type', 'token_search_of_type', 'Search for identifiers of a specific type'),
('token', 'missing', 'token_search_missing', 'Search for resources where the parameter is missing or present');
--;;
CREATE OR REPLACE FUNCTION token_search_base(
    doc jsonb,
    path text,
    token text
)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    system text;
    code text;
    json_path text;
BEGIN
    IF token LIKE '%|%' THEN
        system := split_part(token, '|', 1);
        code := split_part(token, '|', 2);
        
        IF system = '' THEN
            json_path := format('$.%s ? (@.code == "%s")', path, code);
        ELSE
            json_path := format('$.%s ? (@.system == "%s" && @.code == "%s")', path, system, code);
        END IF;
    ELSE
        code := token;
        json_path := format('$.%s ? (@ == "%s" || @.code == "%s")', path, code, code);
    END IF;
    
    RETURN jsonb_path_exists(doc, json_path::jsonpath);
END
$$;
--;;
CREATE OR REPLACE FUNCTION token_search_not(
    doc jsonb,
    path text,
    token text
)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  RETURN NOT token_search_base(doc, path, token);
END
$$;
--;;
CREATE OR REPLACE FUNCTION token_search_missing(
  doc jsonb,
  path text,
  token text
)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
json_path text;
BEGIN
  json_path := format('$.%s', path);
  IF token = 'false' THEN
    RETURN jsonb_path_exists(doc, json_path::jsonpath);
  ELSE 
    RETURN NOT jsonb_path_exists(doc, json_path::jsonpath);
  END IF;
END
$$;
--;;
CREATE OR REPLACE FUNCTION token_search_text(
  doc jsonb,
  path text,
  token text
)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  json_path text;
BEGIN
  json_path:= format('$.%s.** ? ((exists(@.display) && @.display like_regex "%s" flag "i")|| (exists(@.text) && @.text like_regex "%s" flag "i"))', path, token, token);
  RETURN jsonb_path_exists(doc, json_path::jsonpath);
END
$$;
--;;
CREATE OR REPLACE FUNCTION token_search_of_type(
  doc jsonb,
  path text,
  token text
)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  json_path text;
  system text;
  code text;
  value text;
BEGIN
  system := split_part(token, '|', 1);
  code := split_part(token, '|', 2);
  value := split_part(token, '|', 3);
  
  IF system <> '' AND code <> '' AND value <> '' THEN
    json_path := format('$.%s ? (exists(@.type.coding[*] ? (@.system == "%s" && @.code == "%s")) && @.value == "%s")', 
                        path, system, code, value);
  ELSIF code <> '' AND value <> '' THEN
    json_path := format('$.%s ? (exists(@.type.coding[*] ? (@.code == "%s")) && @.value == "%s")', 
                        path, code, value);
  ELSE
    RAISE EXCEPTION 'Invalid :of-type format. Expected: system|code|value or |code|value';
  END IF;
  RETURN jsonb_path_exists(doc, json_path::jsonpath);
END
$$;  
--;;
CREATE OR REPLACE FUNCTION fhir_token_search(
    doc jsonb,
    path text,
    token text,
    modifier text DEFAULT 'base'
)
RETURNS boolean
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  handler_name text;
  result boolean;
BEGIN
  IF token = '' OR token IS NULL THEN
    RAISE EXCEPTION 'Search parameter value cannot be empty';
  END IF;
  SELECT handler_function INTO handler_name FROM fhir_search_functions
  WHERE search_type = 'token' AND fhir_search_functions.modifier = fhir_token_search.modifier;
  IF handler_name IS NULL THEN
    RAISE EXCEPTION 'Unsupported modifier "%" for token search', modifier;
  END IF;
  EXECUTE format('SELECT %I($1, $2, $3)', handler_name) 
  INTO result
  USING doc, path, token;

  RETURN result;
END
$$;
