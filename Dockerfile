FROM ubuntu:22.04

# install wget (for conda) and git
ENV DEBIAN_FRONTEND=noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN=true
RUN apt-get update && apt-get upgrade -y && apt-get install -y wget git

# clone satyrn-api repo
RUN mkdir /satyrn && cd /satyrn && git clone --branch develop https://github.com/scales-okn/satyrn-api.git

# set up conda
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
RUN chmod +x Miniconda3-latest-Linux-x86_64.sh
RUN bash ./Miniconda3-latest-Linux-x86_64.sh -p /miniconda -b
ENV PATH=/miniconda/bin:${PATH}

# set up env (pip_dependencies may need to change if core dev changes requirements.txt in satyrn-api)
# n.b.: zsh users should change "${!pip_dependencies[@]}" to "${(@k)pip_dependencies}"
RUN conda update -y conda
RUN conda init bash
SHELL ["/bin/bash", "-c"]
RUN cd /satyrn/satyrn-api && \
    echo "dependencies:" > requirements.yml && \
    declare -A pip_dependencies=([Flask-Migrate]=1 [Flask-Security-Too==3.4.5]=1) && \
    while read line; do ([[ -z "${pip_dependencies[$line]}" ]] && echo "  - $line" >> requirements.yml); done < requirements.txt && \
    echo "  - pip" >> requirements.yml && echo "  - pip:" >> requirements.yml && \
    for i in "${!pip_dependencies[@]}"; do (echo "      - $i" >> requirements.yml); done && \
    conda env create --name satyrn-api -f requirements.yml

# install gunicorn
RUN conda run --no-capture-output -n satyrn-api pip install gunicorn