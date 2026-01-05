
CREATE OR REPLACE FUNCTION public.try_numeric(p_text text)
 RETURNS numeric
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
DECLARE
  v text;
BEGIN
  IF p_text IS NULL THEN
    RETURN NULL;
  END IF;

  -- Limpia espacios y cualquier símbolo que no sea dígito, coma, punto o guion
  v := regexp_replace(btrim(p_text), '[^0-9,.\-]', '', 'g');

  -- Caso 1: sin comas, decimal con punto (US)
  IF v ~ '^\-?\d+(\.\d+)?$' THEN
    RETURN v::numeric;
  END IF;

  -- Caso 2: con separador de miles por comas correctas, y decimal con punto
  -- Ej: -1,234,567.89  ó  1,234  ó  1,234.0
  IF v ~ '^\-?\d{1,3}(,\d{3})+(\.\d+)?$' THEN
    v := replace(v, ',', '');       -- quitamos miles
    RETURN v::numeric;              -- ya queda con decimal '.'
  END IF;

  RETURN NULL;
EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
END;
$function$
;
