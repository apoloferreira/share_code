
FROM python:3.11.10-slim

# RUN apt-get update && \
#     apt-get install -y --no-install-recommends \
#     nginx \
#     ca-certificates && \
#     apt-get clean && \
#     rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=TRUE
ENV PYTHONDONTWRITEBYTECODE=TRUE

COPY requirements.txt .

# RUN pip3 install --upgrade pip
# RUN pip3 --no-cache-dir install -r requirements.txt
RUN pip3 install -r requirements.txt

ENTRYPOINT ["python3"]
