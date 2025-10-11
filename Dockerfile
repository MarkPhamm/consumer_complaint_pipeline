FROM astrocrpublic.azurecr.io/runtime:3.1-1

# Increase timeout to avoid DagBag import failure
ENV AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=300
