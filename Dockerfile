FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
	PYTHONUNBUFFERED=1

WORKDIR /pum

# Install pum (and its dependencies) from the repository.
COPY requirements/base.txt ./requirements.txt
COPY dist/pum-*.whl /tmp/

RUN python -m pip install --no-cache-dir --upgrade pip \
	&& python -m pip install --no-cache-dir pytest \
	&& python -m pip install --no-cache-dir -r requirements.txt \
	&& python -m pip install --no-cache-dir /tmp/pum-*.whl \
	&& rm -rf /tmp/pum-*.whl

ENV PGSERVICEFILE=/.pg_service.conf

ENTRYPOINT ["pum"]
