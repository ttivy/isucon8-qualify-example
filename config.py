"""
gunicorn configuration file
"""

bind = '127.0.0.1:8080'
workers = 8
worker_class = 'gevent'
