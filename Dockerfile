FROM python:3.10

ARG SRC

WORKDIR /app

COPY sources/$SRC source_dir
COPY bagel bagel

SHELL ["bash","-c"]

RUN pip install -r bagel/requirements.txt
RUN pip install -e ./bagel
RUN pip install -r source_dir/requirements.txt 


CMD ["source_dir/get_data.py"]

ENTRYPOINT ["python"]
