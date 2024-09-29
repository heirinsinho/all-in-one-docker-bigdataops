FROM base-hadoop:latest

ENV HDFS_CONF_dfs_namenode_name_dir=file:///hadoop/dfs/name

RUN mkdir -p /hadoop/dfs/name
VOLUME /hadoop/dfs/name

COPY ./run.sh /run.sh
RUN chmod a+x /run.sh

RUN mkdir /var/run/sshd
RUN echo 'root:root123' | chpasswd
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

EXPOSE 9870 22 9000

RUN env | sed 's/^/export /' > /etc/profile.d/env_vars.sh && chmod +x /etc/profile.d/env_vars.sh

CMD ["/run.sh"]
