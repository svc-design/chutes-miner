FROM parachutes/python:3.12 AS cuda
RUN curl -sSL https://install.python-poetry.org | python -
ENV PATH=$PATH:/home/chutes/.local/bin
ADD --chown=chutes bootstrap.py poetry.lock pyproject.toml /app
WORKDIR /app
RUN poetry install --no-root
ENTRYPOINT ["poetry", "run", "python", "bootstrap.py"]


FROM parachutes/python:3.12.3-rocm AS rocm
RUN curl -sSL https://install.python-poetry.org | python -
ENV PATH=$PATH:/home/chutes/.local/bin
ADD --chown=chutes bootstrap.py poetry.lock pyproject.toml /app
WORKDIR /app
RUN poetry install --no-root
USER chutes
ENTRYPOINT ["poetry", "run", "python", "bootstrap.py"]
