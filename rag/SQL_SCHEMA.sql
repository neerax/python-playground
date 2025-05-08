CREATE EXTENSION "vector";

CREATE TABLE "documenti" (
    uuid UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    path text not null,
    name text not null,
    unique (path, name)
)

CREATE ROLE "service-account-rag"; -- keycloak lo setta un automatico se è un service account (client credential grant)

GRANT SELECT, INSERT, UPDATE, DELETE ON documenti TO "service-account-rag";

GRANT "service-account-rag" to pgtswitcher;

CREATE OR REPLACE FUNCTION pgrst_watch() RETURNS event_trigger
  LANGUAGE plpgsql
  AS $$
BEGIN
  NOTIFY pgrst, 'reload schema';
END;
$$;

-- This event trigger will fire after every ddl_command_end event
CREATE EVENT TRIGGER pgrst_watch
  ON ddl_command_end
  EXECUTE PROCEDURE pgrst_watch();
