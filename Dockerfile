FROM python:3.10

ARG SRC

WORKDIR /app
COPY . .

SHELL ["bash","-c"]

RUN python -m venv venv && \
    source venv/bin/activate && \
    pip install -r bagel/requirements.txt && \
    pip install -e ./bagel && \
    pip install -r sources/$SRC/requirements.txt 

CMD ["sources/$SRC/$SRC.py"]
ENTRYPOINT ["python"]
