FROM python:3
WORKDIR /app
COPY . /app
# Install Python dependencies
RUN pip install -r requirements.txt
EXPOSE 5000
CMD python ./app.py
