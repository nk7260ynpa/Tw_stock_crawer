FROM python:3.12.7

ENV LANG=en_US.UTF-8
ENV TZ=Asia/Taipei
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /workspace

COPY ../requirements.txt /workspace/requirements.txt
RUN pip install -r requirements.txt
COPY ../ /workspace/
RUN python setup.py install

EXPOSE 6738
CMD ["sh", "server.sh"]
