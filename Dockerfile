FROM python
WORKDIR /data
COPY requirements.txt .
RUN set -ex &&\
    pip install -r requirements.txt

ENV PYTHONUNBUFFERED=1
ARG REDIS_URL=redis://redis:6379/0
ENV REDIS_URL=$REDIS_URL
COPY currency.py .
RUN chmod +x currency.py
CMD ["python", "currency.py"]