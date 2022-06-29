FROM python:latest
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
RUN pip install -r sources/[source_name]/requirements.txt
CMD ["sources/[source_name]/[source_name].py"]
ENTRYPOINT ["python3"]
#docker build -t elt-okta . -f sources/okta/Dockerfile 
#docker run elt-okta