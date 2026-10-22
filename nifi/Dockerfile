FROM apache/nifi:1.21.0

ADD --chown=nifi:nifi https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/nifi/drivers/postgresql.jar
COPY --chown=nifi:nifi flow.json.gz /opt/nifi/nifi-current/conf/flow.json.gz
