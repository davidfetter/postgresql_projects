/* contrib/statement_trigger_row--1.0.sql */


-- Complain if script is sourced in psql, rather than via CREATE EXTENSION.
\echo Use "CREATE EXTENSION statement_trigger_row" to load this file.  \quit

CREATE FUNCTION statement_trigger_row()
RETURNS pg_catalog.trigger
AS 'MODULE_PATHNAME'
LANGUAGE C;
