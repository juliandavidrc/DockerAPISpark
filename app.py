from flask import Flask

app = Flask(__name__)

@app.route('/')
def run():
    return 'Hello, World App!'

if __name__ == '__main__':
   app.run()