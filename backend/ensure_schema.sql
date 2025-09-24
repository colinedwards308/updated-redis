-- save as sql/ensure_schema.sql (or inline in code)
DO $$
BEGIN
  -- customers.id -> uuid (if not already)
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='customers'
      AND column_name='id' AND data_type <> 'uuid'
  ) THEN
    EXECUTE 'ALTER TABLE public.customers
               ALTER COLUMN id TYPE uuid USING id::uuid';
  END IF;

  -- customers string columns -> text
  PERFORM 1 FROM information_schema.columns
   WHERE table_schema='public' AND table_name='customers'
     AND column_name IN ('first_name','last_name','email','address','city','state','zip4')
     AND data_type <> 'text';
  IF FOUND THEN
    EXECUTE 'ALTER TABLE public.customers
               ALTER COLUMN first_name TYPE text,
               ALTER COLUMN last_name  TYPE text,
               ALTER COLUMN email      TYPE text,
               ALTER COLUMN address    TYPE text,
               ALTER COLUMN city       TYPE text,
               ALTER COLUMN state      TYPE text,
               ALTER COLUMN zip4       TYPE text';
  END IF;

  -- transactions.id -> uuid
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='transactions'
      AND column_name='id' AND data_type <> 'uuid'
  ) THEN
    EXECUTE 'ALTER TABLE public.transactions
               ALTER COLUMN id TYPE uuid USING id::uuid';
  END IF;

  -- transactions.user_id -> uuid
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='transactions'
      AND column_name='user_id' AND data_type <> 'uuid'
  ) THEN
    EXECUTE 'ALTER TABLE public.transactions
               ALTER COLUMN user_id TYPE uuid USING user_id::uuid';
  END IF;

  -- transactions category_l* -> text
  PERFORM 1 FROM information_schema.columns
   WHERE table_schema='public' AND table_name='transactions'
     AND column_name IN ('category_l1','category_l2','category_l3')
     AND data_type <> 'text';
  IF FOUND THEN
    EXECUTE 'ALTER TABLE public.transactions
               ALTER COLUMN category_l1 TYPE text,
               ALTER COLUMN category_l2 TYPE text,
               ALTER COLUMN category_l3 TYPE text';
  END IF;

  -- transactions unit_price/total_price -> numeric
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='transactions'
      AND column_name='unit_price' AND data_type <> 'numeric'
  ) THEN
    EXECUTE 'ALTER TABLE public.transactions
               ALTER COLUMN unit_price  TYPE numeric USING unit_price::numeric';
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='transactions'
      AND column_name='total_price' AND data_type <> 'numeric'
  ) THEN
    EXECUTE 'ALTER TABLE public.transactions
               ALTER COLUMN total_price TYPE numeric USING total_price::numeric';
  END IF;

  -- (Re)create FK if missing: transactions.user_id -> customers.id
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'transactions_user_id_fkey'
      AND conrelid = 'public.transactions'::regclass
  ) THEN
    -- Make sure current types match; then add FK
    IF (SELECT data_type FROM information_schema.columns
        WHERE table_schema='public' AND table_name='transactions' AND column_name='user_id') = 'uuid'
       AND
       (SELECT data_type FROM information_schema.columns
        WHERE table_schema='public' AND table_name='customers' AND column_name='id') = 'uuid'
    THEN
      EXECUTE 'ALTER TABLE public.transactions
                 ADD CONSTRAINT transactions_user_id_fkey
                 FOREIGN KEY (user_id) REFERENCES public.customers(id)';
    END IF;
  END IF;

END $$;