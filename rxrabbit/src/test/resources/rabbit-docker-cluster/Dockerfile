#see https://github.com/bijukunjummen/docker-rabbitmq-cluster
FROM rabbitmq:3.5.6-management

RUN mkdir /opt/rabbit

ADD startrabbit.sh /opt/rabbit/

RUN chmod a+x /opt/rabbit/startrabbit.sh

EXPOSE 5672
EXPOSE 15672
EXPOSE 25672
EXPOSE 4369
EXPOSE 9100
EXPOSE 9101
EXPOSE 9102
EXPOSE 9103
EXPOSE 9104
EXPOSE 9105

CMD /opt/rabbit/startrabbit.sh