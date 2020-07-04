FROM amancevice/superset

# Switching to root to install the required packages
USER root

ARG HOME=/var/lib/superset/
ENV SUPERSET_HOME=${HOME}

COPY entrypoint.sh /entrypoint.sh
COPY superset_config.py /etc/superset/superset_config.py

# USER root
# RUN chmod +x /entrypoint.sh

# Switching back to using the `superset` user
WORKDIR ${SUPERSET_HOME}
ENTRYPOINT ["sh", "/entrypoint.sh"]
CMD ["gunicorn", "superset:app"]
USER superset
