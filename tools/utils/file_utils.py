import sys, os

def file_get_contents(path):
	with open(path, 'r') as f:
		data = f.read()
		return data

def file_put_contents(path, data):
	with open(path, 'w') as f:
		f.write(data)
