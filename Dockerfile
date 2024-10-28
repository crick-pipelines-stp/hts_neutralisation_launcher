FROM condaforge/miniforge3

# Install apt packages
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    procps \
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

# # Install pip packages
# RUN pip install numpy
# RUN pip install matplotlib
# RUN pip install scipy

# ENV PATH=/opt/conda/bin:$PATH
# WORKDIR /home

# CMD ["bash"]


# celery
# redis
# requests
# pandas
# numpy
# scikit-image