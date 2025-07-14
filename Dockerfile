# Multi-architecture Dockerfile for CMR Audit Scripts
# Supports both M1 Mac (arm64) and Linux (amd64)
FROM --platform=$BUILDPLATFORM fedora:38

# Accept USER argument from build context (can be passed with --build-arg USER=$USER)
ARG USER=user

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PATH="/opt/venv/bin:$PATH"
ENV USER=${USER}

# Install system dependencies including geospatial libraries
RUN dnf update -y && \
    dnf groupinstall -y "Development Tools" && \
    dnf install -y \
        # Basic system packages
        python3 \
        python3-pip \
        python3-devel \
        git \
        bash \
        curl \
        which \
        procps-ng \
        gcc \
        gcc-c++ \
        make \
        cmake \
        pkg-config \
        # Date utilities used in shell scripts
        coreutils \
        findutils \
        # Geospatial and scientific computing libraries
        gdal \
        gdal-devel \
        geos \
        geos-devel \
        proj \
        proj-devel \
        sqlite \
        sqlite-devel \
        # NetCDF and HDF5 libraries
        netcdf \
        netcdf-devel \
        hdf5 \
        hdf5-devel \
        # Additional libraries for scientific computing
        openjpeg2 \
        openjpeg2-devel \
        libtiff \
        libtiff-devel \
        # XML libraries for various parsers
        libxml2 \
        libxml2-devel \
        libxslt \
        libxslt-devel \
        # Compression libraries
        zlib \
        zlib-devel \
        bzip2 \
        bzip2-devel \
        xz \
        xz-devel \
        # SSL/TLS libraries
        openssl \
        openssl-devel \
        # LAPACK/BLAS for numerical computing
        lapack \
        lapack-devel \
        blas \
        blas-devel \
    # Clean up package cache
    && dnf clean all \
    && rm -rf /var/cache/dnf

# Set environment variables for compiled libraries
ENV GDAL_DATA=/usr/share/gdal
ENV PROJ_LIB=/usr/share/proj
ENV GDAL_LIBRARY_PATH=/usr/lib64/libgdal.so
ENV GEOS_LIBRARY_PATH=/usr/lib64/libgeos_c.so

# Create a user for running the application (security best practice)
# Do this BEFORE creating the virtual environment
RUN groupadd -r cmraudit && useradd -r -g cmraudit -d /app -s /bin/bash cmraudit

# Create a virtual environment
RUN python3 -m venv /opt/venv

# Activate virtual environment and upgrade pip
RUN /opt/venv/bin/pip install --upgrade pip setuptools wheel

# Install Python packages with specific versions where needed
RUN /opt/venv/bin/pip install \
    # Core dependencies from the CMR audit scripts
    aiohttp \
    more-itertools \
    python-dotenv \
    backoff \
    requests \
    python-dateutil \
    pandas \
    tabulate \
    compact-json \
    # Geospatial libraries (install these first as they have C dependencies)
    numpy \
    "GDAL==$(gdal-config --version)" \
    pyproj \
    shapely \
    fiona \
    geopandas \
    rioxarray \
    # Scientific computing libraries  
    h5py \
    netCDF4 \
    xarray \
    dask \
    # Weather data libraries (skip eccodes for now as it may not be available)
    cfgrib \
    # AWS and cloud libraries
    boto3 \
    smart_open \
    # Boto3 type stubs for better type hints
    boto3-stubs \
    boto3-stubs-lite[essential] \
    mypy-boto3-s3 \
    # Data processing libraries
    fastparquet \
    # Database and search libraries
    elasticsearch \
    # Validation and utilities
    validators \
    cachetools==5.2.0 \
    mgrs \
    # Additional utilities
    pyyaml \
    jinja2 \
    ruamel.yaml

# Create directories for the application
RUN mkdir -p /app/audit_results \
             /app/logs

# Set working directory
WORKDIR /app

# Clone the repositories
RUN git clone https://github.com/nasa/opera-sds-ops.git /app/opera-sds-ops && \
    git clone https://github.com/nasa/opera-sds-pcm.git /app/opera-sds-pcm

# Set up environment for the CMR audit scripts
ENV PYTHONPATH="/app/opera-sds-pcm:/app/opera-sds-ops:$PYTHONPATH"
ENV PCM_REPO_PATH="/app/opera-sds-pcm"
ENV OPS_REPO_PATH="/app/opera-sds-ops"

# Create a simple test script
RUN echo '#!/bin/bash' > /app/test_environment.sh
RUN echo 'source /opt/venv/bin/activate' >> /app/test_environment.sh
RUN echo 'echo "Python version: $(python --version)"' >> /app/test_environment.sh
RUN echo 'echo "GDAL version: $(gdal-config --version)"' >> /app/test_environment.sh
RUN echo 'echo "PYTHONPATH: $PYTHONPATH"' >> /app/test_environment.sh
RUN echo 'echo "USER: $USER"' >> /app/test_environment.sh
RUN echo 'echo "Repositories:"' >> /app/test_environment.sh
RUN echo 'ls -la /app/ | grep opera' >> /app/test_environment.sh
RUN echo 'echo "Ready for testing!"' >> /app/test_environment.sh
RUN chmod +x /app/test_environment.sh

# FIX: Change ownership of the virtual environment and app directory to cmraudit user
# This fixes the permission issue with pip installs
RUN chown -R cmraudit:cmraudit /opt/venv /app

# Switch to the non-root user
USER cmraudit

# Default command - start bash for interactive testing
CMD ["/bin/bash"]

# Metadata
LABEL maintainer="OPERA SDS Team"
LABEL description="Base Docker image for OPERA CMR audit scripts with full geospatial support"
LABEL version="2.0"
LABEL architecture="multi-arch (amd64, arm64)"
LABEL geospatial="gdal, geos, proj, netcdf, hdf5"
