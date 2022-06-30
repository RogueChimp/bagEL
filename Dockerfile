FROM python:3.10

ARG SRC

WORKDIR /app

COPY sources/$SRC source_dir
COPY bagel bagel

SHELL ["bash","-c"]

# RUN python -m venv venv && \
#    source venv/bin/activate && \
#    pip install -r bagel/requirements.txt && \
#    pip install -e ./bagel && \
#    pip install -r source_dir/requirements.txt 

RUN pip install -r bagel/requirements.txt
RUN pip install -e ./bagel
RUN pip install -r source_dir/requirements.txt 


CMD ["source_dir/get_data.py"]

ENTRYPOINT ["python"]
