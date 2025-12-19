FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
	PYTHONUNBUFFERED=1

WORKDIR /pum

# Install pum (and its dependencies) from the repository.
COPY pyproject.toml README.md LICENSE ./
COPY requirements ./requirements
COPY pum ./pum

RUN python -m pip install --no-cache-dir --upgrade pip \
	&& python -m pip install --no-cache-dir .

ENTRYPOINT ["pum"]
