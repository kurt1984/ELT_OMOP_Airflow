FROM quay.io/astronomer/astro-runtime:9.1.0

# install dbt into a virtual environment
# replace dbt-postgres with the adapter you need
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-duckdb && deactivate

ENV AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES = airflow\.* astro\.*


# for quarto

ARG QUARTO_VERSION="1.3.450"

# install arm version quarto

RUN curl -o quarto-linux-arm64.deb -L https://github.com/quarto-dev/quarto-cli/releases/download/v${QUARTO_VERSION}/quarto-${QUARTO_VERSION}-linux-arm64.deb
#RUN curl -LO https://github.com/quarto-dev/quarto-cli/releases/download/v1.3.340/quarto-1.3.340-linux-amd64.deb
#RUN curl -LO https://quarto.org/download/latest/quarto-linux-amd64.deb

USER root
RUN dpkg -i quarto-linux-arm64.deb

# install tinytex, not working
# RUN wget -qO- "https://yihui.org/tinytex/install-bin-unix.sh" | sh

#This platform doesn't support installation at this time. Please install manually instead.  
#RUN quarto install tinytex

RUN apt-get update \
    && apt-get install -y \
    texlive-full \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*