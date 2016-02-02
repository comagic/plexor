
-- handler function
CREATE FUNCTION plexor_call_handler ()
RETURNS language_handler AS 'plexor' LANGUAGE C;

-- validator function
CREATE FUNCTION plexor_validator (oid)
RETURNS void AS 'plexor' LANGUAGE C;

-- language
CREATE LANGUAGE plexor HANDLER plexor_call_handler VALIDATOR plexor_validator;

-- validator function
CREATE FUNCTION plexor_fdw_validator (text[], oid)
RETURNS boolean AS 'plexor' LANGUAGE C;

-- foreign data wrapper
CREATE FOREIGN DATA WRAPPER plexor VALIDATOR plexor_fdw_validator;

