-- Make countries columns wide enough
ALTER TABLE public.countries ALTER COLUMN flag TYPE text;
ALTER TABLE public.countries ALTER COLUMN name TYPE text;
ALTER TABLE public.countries ALTER COLUMN description TYPE text;

TRUNCATE TABLE public.countries;
