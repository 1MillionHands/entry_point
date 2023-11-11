FROM python:3.11.6-alpine3.18
EXPOSE 5000

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

ENV PYTHONPATH=/ep:/


WORKDIR /ep
COPY . .
RUN apk update && apk add --no-cache git && apk add --no-cache openssh
RUN mkdir /root/.ssh/

# COPY A VALID SSH KEY TO THE BUILD FOLDER
RUN cp github_secret_key /root/.ssh/id_rsa 
RUN echo "Host github.com\n\tStrictHostKeyChecking no\n IdentityFile /root/.ssh/id_rsa" >> /root/.ssh/config
RUN ssh-keyscan github.com >> /root/.ssh/known_hosts
RUN chmod 400 /root/.ssh/id_rsa

# Install pip requirements
RUN python -m pip install -r requirements.txt

# CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--access-logfile", "logs/access.log", "--error-logfile" , "logs/error.log" ,"server:app"]
CMD ["python", "get_posts.py"]