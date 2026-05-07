# Use an official Python base image (keep your Python version)
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    bzip2 \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Miniconda
ENV PATH=/opt/conda/bin:$PATH
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -b -p /opt/conda \
    && rm /tmp/miniconda.sh \
    && /opt/conda/bin/conda clean -t -i -p -y

# Create and activate a conda environment with Python 3.8
RUN conda create -n myenv python=3.9 -y


# Install conda-build and conda-content-trust in the environment
RUN conda install -n myenv conda-build=3.22.0 conda-content-trust=0.1.3 -y

# Ensure all subsequent commands run inside the conda environment
SHELL ["conda", "run", "-n", "myenv", "/bin/bash", "-c"]

# Copy local files into the container
COPY . /app

# Install Python dependencies from requirements.txt
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir tensorflow==2.13.0
RUN pip install --default-timeout=100 --no-cache-dir -r requirements.txt


# Install Jupyter Notebook
RUN pip install notebook

# Expose the Jupyter Notebook port to the host
EXPOSE 8888

# Default command to run Jupyter Notebook inside the conda environment
CMD ["conda", "run", "--no-capture-output", "-n", "myenv", "jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
