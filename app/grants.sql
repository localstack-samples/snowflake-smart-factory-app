-- Gives the installed Native App read access to the tables its dashboard queries.
-- The app's Streamlit runs as the application, so it sees only what is granted to it.
-- Run by `make app` as ACCOUNTADMIN, which owns the app, with the application name passed as `-D app_name=...`.
GRANT USAGE ON DATABASE FACTORY_PIPELINE_DEMO TO APPLICATION <% app_name %>;
GRANT USAGE ON SCHEMA FACTORY_PIPELINE_DEMO.PUBLIC TO APPLICATION <% app_name %>;
GRANT USAGE ON SCHEMA FACTORY_PIPELINE_DEMO.PUBLIC_MARTS TO APPLICATION <% app_name %>;
GRANT SELECT ON TABLE FACTORY_PIPELINE_DEMO.PUBLIC.RAW_SENSOR_DATA TO APPLICATION <% app_name %>;
GRANT SELECT ON TABLE FACTORY_PIPELINE_DEMO.PUBLIC_MARTS.MACHINE_HEALTH_METRICS TO APPLICATION <% app_name %>;
