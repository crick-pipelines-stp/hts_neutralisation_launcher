FROM condaforge/miniforge3

# Install apt packages
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    pkg-config \
    libmysqlclient-dev \
    gcc \
    python3-dev \
    default-libmysqlclient-dev \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set up conda channels with priority
RUN conda config --add channels defaults \
    && conda config --add channels bioconda \
    && conda config --add channels conda-forge \
    && conda config --set channel_priority strict

# Install conda packages
RUN mamba install -y python=3.11
RUN mamba clean --all --yes
ENV PATH=/opt/conda/bin:$PATH

# Copy and install code
COPY . /usr/src
WORKDIR /usr/src
WORKDIR /usr/src/plaque_assay
RUN pip install .
WORKDIR /usr/src
RUN pip install .

CMD ["bash"]
